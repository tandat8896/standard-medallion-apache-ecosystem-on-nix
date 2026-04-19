{
  description = "ETL Lab - Production (no Rust/CodeScene/lldb)";

  nixConfig = {
    extra-substituters = "https://tandat-etl.cachix.org";
    extra-trusted-public-keys = "tandat-etl.cachix.org-1:ozcJtr36PUoskic/WYGMnYIxC/jLxr+HCBrdgyb03io=";
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/107cba9eb4a8d8c9f8e9e61266d78d340867913a";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs, nixpkgs-unstable, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };
      pkgsUnstable = import nixpkgs-unstable {
        inherit system;
        config.allowUnfree = true;
      };

      # Copy Apache Beam patch from flake-full.nix
      apacheBeamPatched = ps: ps.apache-beam.overridePythonAttrs (old: {
        doCheck = false;
        nativeBuildInputs = (old.nativeBuildInputs or []) ++ [ ps.pyyaml ];
        propagatedBuildInputs = (old.propagatedBuildInputs or []) ++ [ ps.sortedcontainers ];
        postInstall = (old.postInstall or "") + ''
          sed -i 's/op\.setup(self\.data_sampler)/op.setup()/g' \
            $out/lib/python*/site-packages/apache_beam/runners/worker/bundle_processor.py
        '';
      });

      pythonEnv = pkgs.python313.withPackages (ps: with ps; [
        pyspark
        (apacheBeamPatched ps)
        sortedcontainers
        avro-python3
        python-dateutil
        kafka-python-ng
        faker
        ruamel-yaml
        psycopg2
      ]);

      # Kafka JARs for Spark
      kafkaJars = [
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar";
          sha256 = "JuKtpS7TLZMQlgdJ2gdPO/qcIDVMKCMD0b2Ht4hWgUw=";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar";
          sha256 = "0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8";
        })
      ];

      postgresJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar";
        sha256 = "GIl2ch6tjoYn622DidUA3MwMm+vYhSaKMEcYAnSmAx4=";
      };

      # Flink JARs
      flinkKafkaJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar";
        sha256 = "1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06";
      };

      kafkaClientsJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar";
        sha256 = "0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8";
      };

      # Flink with Kafka connector
      flink = pkgs.stdenv.mkDerivation rec {
        pname = "flink";
        version = "1.19.1";
        src = pkgs.fetchurl {
          url = "https://archive.apache.org/dist/flink/flink-${version}/flink-${version}-bin-scala_2.12.tgz";
          sha256 = "1kmxk7kkkmr6f88m0a7yxp0zfdxii6sr4gzh68cx8wfaxzrnjqar";
        };
        nativeBuildInputs = [ pkgs.unzip pkgs.python3 ];
        sourceRoot = ".";
        installPhase = ''
          mkdir -p $out
          cp -r flink-${version}/. $out/

          cp ${flinkKafkaJar} $out/lib/flink-connector-kafka-3.2.0-1.19.jar
          cp ${kafkaClientsJar} $out/lib/kafka-clients-3.4.0.jar

          cd $out/opt/python
          ${pkgs.unzip}/bin/unzip -q pyflink.zip -d pyflink-unzipped
          mv pyflink.zip pyflink.zip.bak
          mv pyflink-unzipped pyflink
          chmod +x $out/opt/python/pyflink/pyflink/bin/*.sh
        '';
      };

      runtimeLibs = with pkgs; [
        stdenv.cc.cc.lib
        glibc
        zlib
      ];

    in
    {
      devShells.${system}.default = pkgs.mkShell {
        strictDeps = true;
        NIX_LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath runtimeLibs;
        NIX_LD = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";

        packages = [
          pythonEnv
          pkgs.openjdk11
          pkgs.apacheKafka
          pkgs.zookeeper
          pkgs.postgresql_15
          flink
          pkgs.tree
          pkgs.just
          pkgs.jq
          pkgs.krb5
          pkgs.atlas
          pkgs.cachix
        ];

        shellHook = ''
          export LD_LIBRARY_PATH="/usr/lib/wsl/lib:${pkgs.lib.makeLibraryPath runtimeLibs}:$LD_LIBRARY_PATH"
          export JAVA_HOME="${pkgs.openjdk11.home}"

          # ETL environment variables
          export AIRFLOW_HOME="$PWD/ETL/infrastructure/airflow-data"
          export SPARK_KAFKA_JARS="${pkgs.lib.concatStringsSep "," kafkaJars}"
          export SPARK_POSTGRES_JAR="${postgresJar}"

          # Flink
          export FLINK_HOME="${flink}"
          export PATH="$FLINK_HOME/bin:$PATH"
          export FLINK_CONF_DIR="$PWD/ETL/infrastructure/flink-data/conf"
          export FLINK_LOG_DIR="$PWD/ETL/infrastructure/flink-data/logs"
          export PYTHONPATH="${flink}/opt/python/pyflink:${flink}/opt/python/py4j-0.10.9.7-src.zip:${flink}/opt/python/cloudpickle-2.2.0-src.zip:$PYTHONPATH"
          mkdir -p $FLINK_LOG_DIR

          # Kafka/ZooKeeper paths
          export ZOOCFGDIR="$PWD/ETL/infrastructure/zookeeper-data/conf"
          export ZOO_LOG_DIR="$PWD/ETL/infrastructure/zookeeper-data/logs"
          mkdir -p $ZOOCFGDIR $ZOO_LOG_DIR

          # Kerberos
          export KRB5_CONFIG="$PWD/ETL/infrastructure/kerberos-data/conf/krb5.conf"
          export KRB5_KDC_PROFILE="$PWD/ETL/infrastructure/kerberos-data/conf/kdc.conf"

          echo "ETL Production Shell (no Rust/CodeScene/lldb)"
          echo "Available: Spark, Kafka, PostgreSQL, Atlas, Kerberos"
          echo "Full dev: nix develop ./flake-full.nix"
        '';
      };

      apps.${system}.migrate = {
        type = "app";
        program = "${pkgs.atlas}/bin/atlas";
      };
    };
}



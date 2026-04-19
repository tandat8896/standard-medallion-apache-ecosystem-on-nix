{
  description = "ETL Lab - Production (no Rust/CodeScene/lldb/GPU)";

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

      # Patch apache-beam for PyFlink compatibility
      apacheBeamPatched = ps: ps.apache-beam.overridePythonAttrs (old: {
        doCheck = false;
        nativeBuildInputs = (old.nativeBuildInputs or []) ++ [ ps.pyyaml ];
        propagatedBuildInputs = (old.propagatedBuildInputs or []) ++ [ ps.sortedcontainers ];
        postInstall = (old.postInstall or "") + ''
          # Patch bundle_processor.py: op.setup(self.data_sampler) -> op.setup()
          sed -i 's/op\.setup(self\.data_sampler)/op.setup()/g' \
            $out/lib/python*/site-packages/apache_beam/runners/worker/bundle_processor.py

          echo "✓ Patched apache-beam bundle_processor for PyFlink compatibility"
        '';
      });

      pythonEnv = pkgs.python3.withPackages (ps: [
        ps.numpy
        ps.pytest
        ps.black
        ps.setuptools
        ps.debugpy
        ps.pyspark
        ps.pandas
        ps.polars
        ps.psycopg2-binary
        ps.kafka-python-ng
        ps.faker
        ps.ruamel-yaml
        (apacheBeamPatched ps)  # Use patched apache-beam
        ps.sortedcontainers  # Required by PyFlink/Apache Beam runtime
        ps.avro-python3      # PyFlink 1.19 needs avro-python3, not avro
        ps.python-dateutil   # Required by PyFlink
        ps.pytz              # Required by PyFlink
        ps.pyyaml             # Required by PyFlink for configuration parsing
        ps.wheel               # Required by PyFlink for file system handling
        
      ]);
      runtimeLibs = with pkgs; [
        stdenv.cc.cc.lib
        zlib
      ];
      # Spark Kafka jars
      kafkaJars = [
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar";
          sha256 = "JuKtpS7TLZMQlgdJ2gdPO/qcIDVMKCMD0b2Ht4hWgUw=";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar";
          sha256 = "0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar";
          sha256 = "07gc6g2bppz7g3qwf4h2hzsw21qc97q2g7nv1nmz9zkas2iinps7";
        })
        (pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar";
          sha256 = "10rzjswra1daf71ip9jiw84dixyrlp8y91p6q0d8pr8mfpp0a1ga";
        })
      ];
      # Flink Kafka connector jars
      flinkKafkaJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar";
        sha256 = "1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06";
      };
      kafkaClientsJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar";
        sha256 = "0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8";
      };
      # PostgreSQL JDBC driver for Spark
      postgresJar = pkgs.fetchurl {
        url = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar";
        sha256 = "GIl2ch6tjoYn622DidUA3MwMm+vYhSaKMEcYAnSmAx4=";
      };
      # Flink với kafka connector baked in + unzipped PyFlink
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

          # Copy Kafka JARs
          cp ${flinkKafkaJar} $out/lib/flink-connector-kafka-3.2.0-1.19.jar
          cp ${kafkaClientsJar} $out/lib/kafka-clients-3.4.0.jar

          # Unzip PyFlink để có thể chạy shell scripts
          cd $out/opt/python
          ${pkgs.unzip}/bin/unzip -q pyflink.zip -d pyflink-unzipped
          mv pyflink.zip pyflink.zip.bak
          mv pyflink-unzipped pyflink

          # Make shell scripts executable
          chmod +x $out/opt/python/pyflink/pyflink/bin/*.sh

          # Patch PyFlink for apache-beam 2.69+ compatibility
          # Fix 1: _get_state_cache_size -> _get_state_cache_size_bytes
          # Fix 2: Function signature changed - experiments -> options object
          cat > /tmp/pyflink_beam_patch.py << 'PYPATCH'
import sys
file_path = sys.argv[1]
with open(file_path, 'r') as f:
    content = f.read()

# Fix function name
content = content.replace('_get_state_cache_size(', '_get_state_cache_size_bytes(')

# Fix arguments: wrap experiments list in PipelineOptions
content = content.replace(
    'state_cache_size=sdk_worker_main._get_state_cache_size_bytes(experiments),',
    'state_cache_size=100*1024*1024,  # Fixed 100MB cache'
)

with open(file_path, 'w') as f:
    f.write(content)
PYPATCH

          python /tmp/pyflink_beam_patch.py \
            $out/opt/python/pyflink/pyflink/fn_execution/beam/beam_worker_pool_service.py
        '';
      };
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
          pkgsUnstable.apache-airflow
          pkgs.zookeeper
          pkgs.postgresql_15
          pkgs.tree
          pkgs.just
          pkgs.jq
          flink
          pkgs.krb5
          pkgs.atlas
          pkgs.cachix
        ];
        shellHook = ''
          export LD_LIBRARY_PATH="/usr/lib/wsl/lib:${pkgs.lib.makeLibraryPath runtimeLibs}:$LD_LIBRARY_PATH"
          export JAVA_HOME="${pkgs.openjdk11.home}"

          # Tất cả data dồn vào ETL/infrastructure/<service>-data/
          export AIRFLOW_HOME="$PWD/ETL/infrastructure/airflow-data"
          export SPARK_KAFKA_JARS="${pkgs.lib.concatStringsSep "," kafkaJars}"
          export SPARK_POSTGRES_JAR="${postgresJar}"
          export FLINK_HOME="${flink}"
          export PATH="$FLINK_HOME/bin:$PATH"
          export FLINK_CONF_DIR="$PWD/ETL/infrastructure/flink-data/conf"
          export FLINK_LOG_DIR="$PWD/ETL/infrastructure/flink-data/logs"
          export FLINK_ENV_JAVA_OPTS="-cp /home/tandat8896-nix/tandat-interview/ETL/infrastructure/flink-data/lib/kafka-clients-4.1.0.jar"

          # Add PyFlink to PYTHONPATH for flink run -py support (unzipped folder)
          export PYTHONPATH="${flink}/opt/python/pyflink:${flink}/opt/python/py4j-0.10.9.7-src.zip:${flink}/opt/python/cloudpickle-2.2.0-src.zip:$PYTHONPATH"

          mkdir -p $AIRFLOW_HOME
          mkdir -p $FLINK_LOG_DIR
          export ZOOCFGDIR="$PWD/ETL/infrastructure/zookeeper-data/conf"
          export ZOO_LOG_DIR="$PWD/ETL/infrastructure/zookeeper-data/logs"
          mkdir -p $ZOOCFGDIR
          mkdir -p $ZOO_LOG_DIR
          export KRB5_CONFIG="$PWD/ETL/infrastructure/kerberos-data/conf/krb5.conf"
          export KRB5_KDC_PROFILE="$PWD/ETL/infrastructure/kerberos-data/conf/kdc.conf"
        '';
      };

      apps.${system}.migrate = {
        type = "app";
        program = "${pkgs.atlas}/bin/atlas";
      };
    };
}

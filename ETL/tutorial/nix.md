❄️ tandat-interview
❯ nix-prefetch-url https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar
path is '/nix/store/caalhr8nbcjnlqlyarn21sm0058fa419-spark-sql-kafka-0-10_2.12-3.5.5.jar'
0k41as4bg1xxs41j6a2c6lh9ryiv9w3xlj87jq896bfk5sjsvqi6

lấy hash của maven


❄️ tandat-interview/ETL
❯ nix-prefetch-url https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

nix-prefetch-url https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar
path is '/nix/store/8bk07zpg06xpf2yb9q4jkq14xn4pmqv8-kafka-clients-3.4.0.jar'
0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8
path is '/nix/store/yhh0q02wgy4dmwhpxia1acmzdh4b3yrr-spark-token-provider-kafka-0-10_2.12-3.5.5.jar'
07gc6g2bppz7g3qwf4h2hzsw21qc97q2g7nv1nmz9zkas2iinps7

❄️ tandat-interview/ETL
❯                                                              on  main [!?]


kafkaJars = [
  (pkgs.fetchurl {
    url = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar";
    sha256 = "0k41as4bg1xxs41j6a2c6lh9ryiv9w3xlj87jq896bfk5sjsvqi6";
  })
  (pkgs.fetchurl {
    url = "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar";
    sha256 = "0r5c6c1kbfkxfabqj9gwa6b8db1gnyvayw7214vyvwlvwvnqvws8";
  })
  (pkgs.fetchurl {
    url = "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar";
    sha256 = "07gc6g2bppz7g3qwf4h2hzsw21qc97q2g7nv1nmz9zkas2iinps7";
  })
];


shellHook = ''
  export LD_LIBRARY_PATH="/usr/lib/wsl/lib:${pkgs.lib.makeLibraryPath runtimeLibs}:$LD_LIBRARY_PATH"
  export RUST_SRC_PATH="${rustToolchain}/lib/rustlib/src/rust/library"
  export JAVA_HOME="${pkgs.openjdk11.home}"
  export AIRFLOW_HOME="$PWD/.airflow"
  mkdir -p $AIRFLOW_HOME

  export SPARK_KAFKA_JARS="${pkgs.lib.concatMapStringsSep "," (jar: "${jar}") kafkaJars}"
'';


spark-submit \
  --master spark://127.0.0.1:7077 \
  --jars $SPARK_KAFKA_JARS \
  src/streaming/consumer.py



❄️ tandat-interview/ETL
❯ nix-prefetch-url https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
path is '/nix/store/zmhjiwplbnr7h5kgwxfnljygxmla41lk-commons-pool2-2.11.1.jar'
10rzjswra1daf71ip9jiw84dixyrlp8y91p6q0d8pr8mfpp0a1ga


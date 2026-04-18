❄️ tandat-interview
❯ which flink                                        on  main [!?]
/nix/store/viym1s3628nn7g6735rfkx6qchgni3sk-flink-1.19.1/bin/flink

❄️ tandat-interview
❯ cd /nix/store/viym1s3628nn7g6735rfkx6qchgni3sk-flink-1.19.1/bin/
direnv: unloading

❄️ store/viym1s3628nn7g6735rfkx6qchgni3sk-flink-1.19.1/bin
❯ ls -la
total 2380
dr-xr-xr-x  2 root root    4096 Jan  1  1970 .
dr-xr-xr-x 10 root root    4096 Jan  1  1970 ..
-r--r--r--  2 root root 2290704 Jan  1  1970 bash-java-utils.jar
-r-xr-xr-x  2 root root    7838 Jan  1  1970 bash-java-utils.sh
-r-xr-xr-x  2 root root    1584 Jan  1  1970 config-parser-utils.sh
-r-xr-xr-x  2 root root   20735 Jan  1  1970 config.sh
-r-xr-xr-x  3 root root    1364 Jan  1  1970 find-flink-home.sh
-r-xr-xr-x  3 root root    2427 Jan  1  1970 flink
-r-xr-xr-x  3 root root    4768 Jan  1  1970 flink-console.sh
-r-xr-xr-x  3 root root    6829 Jan  1  1970 flink-daemon.sh
-r-xr-xr-x  3 root root    1610 Jan  1  1970 historyserver.sh
-r-xr-xr-x  2 root root    2544 Jan  1  1970 jobmanager.sh
-r-xr-xr-x  3 root root    1696 Jan  1  1970 kubernetes-jobmanager.sh
-r-xr-xr-x  3 root root    1891 Jan  1  1970 kubernetes-session.sh
-r-xr-xr-x  3 root root    1816 Jan  1  1970 kubernetes-taskmanager.sh
-r-xr-xr-x  2 root root    1934 Jan  1  1970 migrate-config-file.sh
-r-xr-xr-x  3 root root    3048 Jan  1  1970 pyflink-shell.sh
-r-xr-xr-x  2 root root    4212 Jan  1  1970 sql-client.sh
-r-xr-xr-x  3 root root    3519 Jan  1  1970 sql-gateway.sh
-r-xr-xr-x  3 root root    2052 Jan  1  1970 standalone-job.sh
-r-xr-xr-x  3 root root    1883 Jan  1  1970 start-cluster.sh
-r-xr-xr-x  3 root root    1900 Jan  1  1970 start-zookeeper-quorum.sh
-r-xr-xr-x  3 root root    1663 Jan  1  1970 stop-cluster.sh
-r-xr-xr-x  3 root root    1891 Jan  1  1970 stop-zookeeper-quorum.sh
-r-xr-xr-x  3 root root    3006 Jan  1  1970 taskmanager.sh
-r-xr-xr-x  3 root root    1899 Jan  1  1970 yarn-session.sh
-r-xr-xr-x  3 root root    2451 Jan  1  1970 zookeeper.sh


cat $(which flink) | grep "HA"

echo "Starting HA cluster with ${#MASTERS[@]} masters."

for ((i=0;i<${#MASTERS[@]};++i)); do
    master=${MASTERS[i]}
    webuiport=${WEBUIPORTS[i]}

    if [ ${MASTERS_ALL_LOCALHOST} = true ] ; then
        "${FLINK_BIN_DIR}"/jobmanager.sh start "${master}" "${webuiport}"
    else
        ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager.sh\" start ${master} ${webuiport} &"
    fi
done
❯ install -m 644 $FLINK_HOME/conf/config.yaml infrastructure/flink-data/conf
install: Lệnh dùng để sao chép file và đồng thời thiết lập các thuộc tính (quyền, chủ sở hữu).
-m 644 (Mode): Đây là phần quan trọng nhất. 
Nó thiết lập quyền truy cập cho file ngay khi vừa được copy sang chỗ mới.
6 (Chủ sở hữu ): Có quyền Đọc và Ghi ($4 + 2 = 6$).
4 (Nhóm): Chỉ có quyền Đọc.
4 (Người khác): Chỉ có quyền Đọc.
install: Lệnh dùng để sao chép file và đồng thời thiết lập các thuộc tính (quyền, chủ sở hữu).


high-availability:
  type: zookeeper
  storageDir: file:///home/tandat8896-nix/tandat-interview/ETL/infrastructure/flink-data/ha
  zookeeper:
    quorum: localhost:2181
    client:
      acl: open

execution:
  checkpointing:
    interval: 30s
    mode: EXACTLY_ONCE
    externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

state:
  backend:
    type: hashmap
    incremental: false
  checkpoints:
    dir: file:///home/tandat8896-nix/tandat-interview/ETL/infrastructure/flink-data/checkpoints
  savepoints:
    dir: file:///home/tandat8896-nix/tandat-interview/ETL/infrastructure/flink-data/savepoints

❄️ tandat-interview/ETL
❯ which zkServer.sh                                       on  main [!?]
/nix/store/snvyhc62y9jldc3m6adan6wgd69861pl-zookeeper-3.9.4/bin/zkServer.sh

tìm zookeeper để chạy HA cluster của flink, sau đó copy file config.yaml vào thư mục conf của flink, nội dung file config.yaml đã được chỉnh sửa để cấu hình HA cluster với zookeeper.

❄️ tandat-interview/ETL/infrastructure
❯ nix-prefetch-url https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar
path is '/nix/store/1yb7ckpvfwgi92y8n3q3rzi0w9nmirf5-flink-connector-kafka-3.2.0-1.19.jar'
1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06

flinkKafkaJar = pkgs.fetchurl {
  url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar";
  sha256 = "1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06";
};

❄️ tandat-interview
❯ install -m 644 /nix/store/snvyhc62y9jldc3m6adan6wgd69861pl-zookeeper-3.9.4/conf/zoo_sample.cfg infrastructure/zookeeper-data/conf/zoo.cfg
install: cannot create regular file 'infrastructure/zookeeper-data/conf/zoo.cfg': No such file or directory

❄️ tandat-interview
❯ mkdir -p infrastructure/zookeeper-data/{conf,data,logs} on  main [!?]

❄️ tandat-interview
❯ cd ETL                                                  on  main [!?]

❄️ tandat-interview/ETL
❯ mkdir -p infrastructure/zookeeper-data/{conf,data,logs} on  main [!?]

❄️ tandat-interview/ETL
❯ install -m 644 /nix/store/snvyhc62y9jldc3m6adan6wgd69861pl-zookeeper-3.9.4/conf/zoo_sample.cfg infrastructure/zookeeper-data/conf/zoo.cfg

❄️ tandat-interview/ETL
❯ sed -i 's|dataDir=/tmp/zookeeper|dataDir='$PWD'/infrastructure/zookeeper-data/data|' infrastructure/zookeeper-data/conf/zoo.cfg
echo "dataLogDir=$PWD/infrastructure/zookeeper-data/logs" >> infrastructure/zookeeper-data/conf/zoo.cfg

❄️ tandat-interview/ETL
❯                                                         on  main [!?]

❄️ tandat-interview/ETL
❯ zkServer.sh --help                                      on  main [!?]
ZooKeeper JMX enabled by default
Using config: /home/tandat8896-nix/tandat-interview/ETL/infrastructure/zookeeper-data/conf/zoo.cfg
Usage: /nix/store/snvyhc62y9jldc3m6adan6wgd69861pl-zookeeper-3.9.4/bin/.zkServer.sh-wrapped [--config <conf-dir>] {start|start-foreground|stop|version|restart|status|print-cmd}

❄️ tandat-interview/ETL
❯ zkServer.sh start                                       on  main [!?]
ZooKeeper JMX enabled by default
Using config: /home/tandat8896-nix/tandat-interview/ETL/infrastructure/zookeeper-data/conf/zoo.cfg
Starting zookeeper ... STARTED

❄️ tandat-interview/ETL
❯ start-cluster.sh                                        on  main [!?]
Starting HA cluster with 0 masters.

❄️ tandat-interview/ETL
❯ cat infrastructure/flink-data/conf/masters              on  main [!?]

❄️ tandat-interview/ETL
❯ echo "localhost" > infrastructure/flink-data/conf/masters

❄️ tandat-interview/ETL
❯ start-cluster.sh                                        on  main [!?]
Starting HA cluster with 1 masters.
Starting standalonesession daemon on host nixos.

❄️ tandat-interview/ETL
❯                                                         on  main [!?]

❄️ tandat-interview/ETL
❯ echo "localhost" > infrastructure/flink-data/conf/masters

❄️ tandat-interview/ETL
❯ start-cluster.sh                                        on  main [!?]
Starting HA cluster with 1 masters.
Starting standalonesession daemon on host nixos.

❄️ tandat-interview/ETL
❯ cat infrastructure/flink-data/conf/workers              on  main [!?]

❄️ tandat-interview/ETL
❯ echo "localhost" > infrastructure/flink-data/conf/workers
start-cluster.sh
Starting HA cluster with 1 masters.
[INFO] 1 instance(s) of standalonesession are already running on nixos.
Starting standalonesession daemon on host nixos.
Starting taskexecutor daemon on host nixos.

❄️ tandat-interview/ETL
❯                                                         on  main [!?]
❄️ tandat-interview/ETL
❯ zkServer.sh --help                                      on  main [!?]
ZooKeeper JMX enabled by default
Using config: /home/tandat8896-nix/tandat-interview/ETL/infrastructure/zookeeper-data/conf/zoo.cfg
Usage: /nix/store/snvyhc62y9jldc3m6adan6wgd69861pl-zookeeper-3.9.4/bin/.zkServer.sh-wrapped [--config <conf-dir>] {start|start-foreground|stop|version|restart|status|print-cmd}

❄️ tandat-interview/ETL
❯ zkServer.sh start                                       on  main [!?]
ZooKeeper JMX enabled by default
Using config: /home/tandat8896-nix/tandat-interview/ETL/infrastructure/zookeeper-data/conf/zoo.cfg
Starting zookeeper ... STARTED

❄️ tandat-interview/ETL
❯ start-cluster.sh                                        on  main [!?]
Starting HA cluster with 0 masters.

❄️ tandat-interview/ETL
❯ cat infrastructure/flink-data/conf/masters              on  main [!?]

❄️ tandat-interview/ETL
❯ echo "localhost" > infrastructure/flink-data/conf/masters

❄️ tandat-interview/ETL
❯ start-cluster.sh                                        on  main [!?]
Starting HA cluster with 1 masters.
Starting standalonesession daemon on host nixos.

❄️ tandat-interview/ETL
❯ cat infrastructure/flink-data/conf/workers              on  main [!?]

❄️ tandat-interview/ETL
❯ echo "localhost" > infrastructure/flink-data/conf/workers
start-cluster.sh
Starting HA cluster with 1 masters.
[INFO] 1 instance(s) of standalonesession are already running on nixos.
Starting standalonesession daemon on host nixos.
Starting taskexecutor daemon on host nixos.

❄️ tandat-interview/ETL
❯ curl -s http://localhost:8081/overview                  on  main [!?]

❄️ tandat-interview/ETL
❯                                                         on  main [!?]

❄️ tandat-interview/ETL
❯ start-cluster.sh                                        on  main [!?]
Starting HA cluster with 1 masters.
[INFO] 2 instance(s) of standalonesession are already running on nixos.
Starting standalonesession daemon on host nixos.
[INFO] 1 instance(s) of taskexecutor are already running on nixos.
Starting taskexecutor daemon on host nixos.

❄️ tandat-interview/ETL
❯ curl -s http://localhost:8081/overview                  on  main [!?]

❄️ tandat-interview/ETL
❯ ls infrastructure/flink-data/logs/                      on  main [!?]
flink-tandat8896-nix-client-nixos.log
flink-tandat8896-nix-standalonesession-0-nixos.log
flink-tandat8896-nix-standalonesession-0-nixos.out
flink-tandat8896-nix-standalonesession-1-nixos.out
flink-tandat8896-nix-standalonesession-2-nixos.out
flink-tandat8896-nix-taskexecutor-0-nixos.log
flink-tandat8896-nix-taskexecutor-0-nixos.out
flink-tandat8896-nix-taskexecutor-1-nixos.out

❄️ tandat-interview/ETL
❯ tail -30 infrastructure/flink-data/logs/flink-tandat8896-nix-standalonesession-0-nixos.log
2026-04-17 22:51:52,636 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint   [] - Rest endpoint listening at 127.0.0.1:8081
2026-04-17 22:51:52,637 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint   [] - http://127.0.0.1:8081 was granted leadership with leaderSessionID=00000000-0000-0000-0000-000000000000
2026-04-17 22:51:52,638 INFO  org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint   [] - Web frontend listening at http://127.0.0.1:8081.
2026-04-17 22:51:52,656 INFO  org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunner [] - DefaultDispatcherRunner was granted leadership with leader id 00000000-0000-0000-0000-000000000000. Creating new DispatcherLeaderProcess.
2026-04-17 22:51:52,662 INFO  org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess [] - Start SessionDispatcherLeaderProcess.
2026-04-17 22:51:52,664 INFO  org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl [] - Starting resource manager service.
2026-04-17 22:51:52,666 INFO  org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl [] - Resource manager service is granted leadership with session id 00000000-0000-0000-0000-000000000000.
2026-04-17 22:51:52,667 INFO  org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess [] - Recover all persisted job graphs that are not finished, yet.
2026-04-17 22:51:52,668 INFO  org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess [] - Successfully recovered 0 persisted job graphs.
2026-04-17 22:51:52,686 INFO  org.apache.flink.runtime.rpc.pekko.PekkoRpcService           [] - Starting RPC endpoint for org.apache.flink.runtime.dispatcher.StandaloneDispatcher at pekko://flink/user/rpc/dispatcher_0 .
2026-04-17 22:51:52,687 INFO  org.apache.flink.runtime.rpc.pekko.PekkoRpcService           [] - Starting RPC endpoint for org.apache.flink.runtime.resourcemanager.StandaloneResourceManager at pekko://flink/user/rpc/resourcemanager_1 .
2026-04-17 22:51:52,697 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager [] - Starting the resource manager.
2026-04-17 22:51:52,703 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager [] - Starting the slot manager.
2026-04-17 22:51:52,704 INFO  org.apache.flink.runtime.security.token.DefaultDelegationTokenManager [] - Starting tokens update task
2026-04-17 22:51:52,705 WARN  org.apache.flink.runtime.security.token.DefaultDelegationTokenManager [] - No tokens obtained so skipping notifications
2026-04-17 22:51:52,705 WARN  org.apache.flink.runtime.security.token.DefaultDelegationTokenManager [] - Tokens update task not started because either no tokens obtained or none of the tokens specified its renewal date
2026-04-17 22:51:55,470 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager [] - Registering TaskManager with ResourceID localhost:33241-4b9a22 (pekko.tcp://flink@localhost:33241/user/rpc/taskmanager_0) at ResourceManager
2026-04-17 22:51:55,497 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager [] - Registering task executor localhost:33241-4b9a22 under 69d76b0413ec7117253cc2b5995f0b9e at the slot manager.
2026-04-17 22:52:50,422 INFO  org.apache.flink.runtime.resourcemanager.StandaloneResourceManager [] - Closing TaskExecutor connection localhost:33241-4b9a22 because: The TaskExecutor is shutting down.
2026-04-17 22:52:50,422 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager [] - Unregistering task executor 69d76b0413ec7117253cc2b5995f0b9e from the slot manager.
2026-04-17 22:52:53,519 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - RECEIVED SIGNAL 15: SIGTERM. Shutting down as requested.
2026-04-17 22:52:53,524 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] - Shutting StandaloneSessionClusterEntrypoint down with application status UNKNOWN. Diagnostics Cluster entrypoint has been closed externally..
2026-04-17 22:52:53,528 INFO  org.apache.flink.runtime.blob.BlobServer                     [] - Stopped BLOB server at 127.0.0.1:44425
2026-04-17 22:52:53,669 INFO  org.apache.pekko.remote.RemoteActorRefProvider$RemotingTerminator [] - Shutting down remote daemon.
2026-04-17 22:52:53,669 INFO  org.apache.pekko.actor.CoordinatedShutdown                   [] - Running CoordinatedShutdown with reason [ActorSystemTerminateReason]
2026-04-17 22:52:53,671 INFO  org.apache.pekko.remote.RemoteActorRefProvider$RemotingTerminator [] - Remote daemon shut down; proceeding with flushing remote transports.
2026-04-17 22:52:53,673 INFO  org.apache.pekko.remote.RemoteActorRefProvider$RemotingTerminator [] - Shutting down remote daemon.
2026-04-17 22:52:53,673 INFO  org.apache.pekko.remote.RemoteActorRefProvider$RemotingTerminator [] - Remote daemon shut down; proceeding with flushing remote transports.
2026-04-17 22:52:53,692 INFO  org.apache.pekko.remote.RemoteActorRefProvider$RemotingTerminator [] - Remoting shut down.
2026-04-17 22:52:53,692 INFO  org.apache.pekko.remote.RemoteActorRefProvider$RemotingTerminator [] - Remoting shut down.

❄️ tandat-interview/ETL
❯ cat infrastructure/flink-data/logs/flink-tandat8896-nix-standalonesession-0-nixos.out
ERROR StatusLogger Reconfiguration failed: No configuration found for '5c29bfd' at 'null' in 'null'
ERROR StatusLogger Reconfiguration failed: No configuration found for '5dcb4f5f' at 'null' in 'null'
00:06:37.761 [main-EventThread] ERROR org.apache.flink.shaded.curator5.org.apache.curator.ConnectionState - Authentication failed

❄️ tandat-interview/ETL
❯ zkServer.sh status                                      on  main [!?]
ZooKeeper JMX enabled by default
Using config: /home/tandat8896-nix/tandat-interview/ETL/infrastructure/zookeeper-data/conf/zoo.cfg
Client port found: 2181. Client address: localhost. Client SSL: false.
Mode: standalone

❄️ tandat-interview/ETL
❯ ls /nix/store/snvyhc62y9jldc3m6adan6wgd69861pl-zookeeper-3.9.4/conf/
configuration.xsl  logback.xml  zoo_sample.cfg

❄️ tandat-interview/ETL
❯                                                         on  main [!?]
$FLINK_HOME/lib/flink-csv-1.19.1.jar
❄️ tandat-interview
❯ jar tf $FLINK_HOME/lib/flink-dist-1.19.1.jar | grep jaas
flink-jaas.conf

❄️ tandat-interview/ETL
❯ nix eval --raw 'nixpkgs#krb5' --extra-experimental-features 'nix-command flakes' 2>/dev/null
/nix/store/cwmz3yfb7kqafg8ssd3jgypgbml41wlm-krb5-1.22.1%


kadmin.local -d infrastructure/kerberos-data/data/principal -r ETL.LOCAL -q "ktadd -k infrastructure/kerberos-data/data/flink.keytab flink/localhost@ETL.LOCAL"
mo comment security ra 


❄️ tandat-interview/ETL
❯ grep -n "webui-port" $FLINK_HOME/bin/jobmanager.sh | head -5                on  main [!?]
63:        args+=("--webui-port")

❄️ tandat-interview/ETL
❯ sed -n '55,75p' $FLINK_HOME/bin/jobmanager.sh                               on  main [!?]

    args=("--configDir" "${FLINK_CONF_DIR}" "--executionMode" "cluster" "${args[@]}")
    if [ ! -z $HOST ]; then
        args+=("--host")
        args+=("${HOST}")
    fi

    if [ ! -z $WEBUIPORT ]; then
        args+=("--webui-port")
        args+=("${WEBUIPORT}")
    fi

    if [ ! -z "${DYNAMIC_PARAMETERS}" ]; then
        args=(${DYNAMIC_PARAMETERS[@]} "${args[@]}")
    fi
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${args[@]}"
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${args[@]}"

❄️ tandat-interview/ETL



thieu file log4j
❯❄️ tandat-interview/ETL
❯ which flink                                        on  feat/medallion-logic [!]
/nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin/flink

❄️ tandat-interview/ETL
❯ cd /nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin/
direnv: unloading

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin
❯ ls -la
total 2380
dr-xr-xr-x  2 root root    4096 Jan  1  1970 .
dr-xr-xr-x 10 root root    4096 Jan  1  1970 ..
-r--r--r--  3 root root 2290704 Jan  1  1970 bash-java-utils.jar
-r-xr-xr-x  3 root root    7838 Jan  1  1970 bash-java-utils.sh
-r-xr-xr-x  3 root root    1584 Jan  1  1970 config-parser-utils.sh
-r-xr-xr-x  3 root root   20735 Jan  1  1970 config.sh
-r-xr-xr-x  4 root root    1364 Jan  1  1970 find-flink-home.sh
-r-xr-xr-x  4 root root    2427 Jan  1  1970 flink
-r-xr-xr-x  4 root root    4768 Jan  1  1970 flink-console.sh
-r-xr-xr-x  4 root root    6829 Jan  1  1970 flink-daemon.sh
-r-xr-xr-x  4 root root    1610 Jan  1  1970 historyserver.sh
-r-xr-xr-x  3 root root    2544 Jan  1  1970 jobmanager.sh
-r-xr-xr-x  4 root root    1696 Jan  1  1970 kubernetes-jobmanager.sh
-r-xr-xr-x  4 root root    1891 Jan  1  1970 kubernetes-session.sh
-r-xr-xr-x  4 root root    1816 Jan  1  1970 kubernetes-taskmanager.sh
-r-xr-xr-x  3 root root    1934 Jan  1  1970 migrate-config-file.sh
-r-xr-xr-x  4 root root    3048 Jan  1  1970 pyflink-shell.sh
-r-xr-xr-x  3 root root    4212 Jan  1  1970 sql-client.sh
-r-xr-xr-x  4 root root    3519 Jan  1  1970 sql-gateway.sh
-r-xr-xr-x  4 root root    2052 Jan  1  1970 standalone-job.sh
-r-xr-xr-x  4 root root    1883 Jan  1  1970 start-cluster.sh
-r-xr-xr-x  4 root root    1900 Jan  1  1970 start-zookeeper-quorum.sh
-r-xr-xr-x  4 root root    1663 Jan  1  1970 stop-cluster.sh
-r-xr-xr-x  4 root root    1891 Jan  1  1970 stop-zookeeper-quorum.sh
-r-xr-xr-x  4 root root    3006 Jan  1  1970 taskmanager.sh
-r-xr-xr-x  4 root root    1899 Jan  1  1970 yarn-session.sh
-r-xr-xr-x  4 root root    2451 Jan  1  1970 zookeeper.sh

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin
❯ cd ..

❄️ /nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1
❯ ls -la
total 2048
dr-xr-xr-x   10 root root      4096 Jan  1  1970 .
drwxrwxr-t 1837 root nixbld 1892352 Apr 18 01:13 ..
dr-xr-xr-x    2 root root      4096 Jan  1  1970 bin
dr-xr-xr-x    2 root root      4096 Jan  1  1970 conf
dr-xr-xr-x    6 root root      4096 Jan  1  1970 examples
dr-xr-xr-x    2 root root      4096 Jan  1  1970 lib
-r--r--r--   25 root root     11357 Jan  1  1970 LICENSE
dr-xr-xr-x    2 root root      4096 Jan  1  1970 licenses
dr-xr-xr-x    2 root root      4096 Jan  1  1970 log
-r--r--r--    3 root root    144074 Jan  1  1970 NOTICE
dr-xr-xr-x    3 root root      4096 Jan  1  1970 opt
dr-xr-xr-x   10 root root      4096 Jan  1  1970 plugins
-r--r--r--    4 root root      1309 Jan  1  1970 README.txt

❄️ /nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1
❯ cd conf

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/conf
❯ ls -la
total 64
dr-xr-xr-x  2 root root  4096 Jan  1  1970 .
dr-xr-xr-x 10 root root  4096 Jan  1  1970 ..
-r--r--r--  3 root root 14428 Jan  1  1970 config.yaml
-r--r--r--  4 root root  2940 Jan  1  1970 log4j-cli.properties
-r--r--r--  4 root root  3207 Jan  1  1970 log4j-console.properties
-r--r--r--  4 root root  2731 Jan  1  1970 log4j.properties
-r--r--r--  4 root root  2064 Jan  1  1970 log4j-session.properties
-r--r--r--  4 root root  2884 Jan  1  1970 logback-console.xml
-r--r--r--  4 root root  1569 Jan  1  1970 logback-session.xml
-r--r--r--  4 root root  2333 Jan  1  1970 logback.xml
-r--r--r--  4 root root    15 Jan  1  1970 masters
-r--r--r--  4 root root    10 Jan  1  1970 workers
-r--r--r--  4 root root  1434 Jan  1  1970 zoo.cfg

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/conf
❯❄️ tandat-interview/ETL
❯ which flink                                        on  feat/medallion-logic [!]
/nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin/flink

❄️ tandat-interview/ETL
❯ cd /nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin/
direnv: unloading

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin
❯ ls -la
total 2380
dr-xr-xr-x  2 root root    4096 Jan  1  1970 .
dr-xr-xr-x 10 root root    4096 Jan  1  1970 ..
-r--r--r--  3 root root 2290704 Jan  1  1970 bash-java-utils.jar
-r-xr-xr-x  3 root root    7838 Jan  1  1970 bash-java-utils.sh
-r-xr-xr-x  3 root root    1584 Jan  1  1970 config-parser-utils.sh
-r-xr-xr-x  3 root root   20735 Jan  1  1970 config.sh
-r-xr-xr-x  4 root root    1364 Jan  1  1970 find-flink-home.sh
-r-xr-xr-x  4 root root    2427 Jan  1  1970 flink
-r-xr-xr-x  4 root root    4768 Jan  1  1970 flink-console.sh
-r-xr-xr-x  4 root root    6829 Jan  1  1970 flink-daemon.sh
-r-xr-xr-x  4 root root    1610 Jan  1  1970 historyserver.sh
-r-xr-xr-x  3 root root    2544 Jan  1  1970 jobmanager.sh
-r-xr-xr-x  4 root root    1696 Jan  1  1970 kubernetes-jobmanager.sh
-r-xr-xr-x  4 root root    1891 Jan  1  1970 kubernetes-session.sh
-r-xr-xr-x  4 root root    1816 Jan  1  1970 kubernetes-taskmanager.sh
-r-xr-xr-x  3 root root    1934 Jan  1  1970 migrate-config-file.sh
-r-xr-xr-x  4 root root    3048 Jan  1  1970 pyflink-shell.sh
-r-xr-xr-x  3 root root    4212 Jan  1  1970 sql-client.sh
-r-xr-xr-x  4 root root    3519 Jan  1  1970 sql-gateway.sh
-r-xr-xr-x  4 root root    2052 Jan  1  1970 standalone-job.sh
-r-xr-xr-x  4 root root    1883 Jan  1  1970 start-cluster.sh
-r-xr-xr-x  4 root root    1900 Jan  1  1970 start-zookeeper-quorum.sh
-r-xr-xr-x  4 root root    1663 Jan  1  1970 stop-cluster.sh
-r-xr-xr-x  4 root root    1891 Jan  1  1970 stop-zookeeper-quorum.sh
-r-xr-xr-x  4 root root    3006 Jan  1  1970 taskmanager.sh
-r-xr-xr-x  4 root root    1899 Jan  1  1970 yarn-session.sh
-r-xr-xr-x  4 root root    2451 Jan  1  1970 zookeeper.sh

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/bin
❯ cd ..

❄️ /nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1
❯ ls -la
total 2048
dr-xr-xr-x   10 root root      4096 Jan  1  1970 .
drwxrwxr-t 1837 root nixbld 1892352 Apr 18 01:13 ..
dr-xr-xr-x    2 root root      4096 Jan  1  1970 bin
dr-xr-xr-x    2 root root      4096 Jan  1  1970 conf
dr-xr-xr-x    6 root root      4096 Jan  1  1970 examples
dr-xr-xr-x    2 root root      4096 Jan  1  1970 lib
-r--r--r--   25 root root     11357 Jan  1  1970 LICENSE
dr-xr-xr-x    2 root root      4096 Jan  1  1970 licenses
dr-xr-xr-x    2 root root      4096 Jan  1  1970 log
-r--r--r--    3 root root    144074 Jan  1  1970 NOTICE
dr-xr-xr-x    3 root root      4096 Jan  1  1970 opt
dr-xr-xr-x   10 root root      4096 Jan  1  1970 plugins
-r--r--r--    4 root root      1309 Jan  1  1970 README.txt

❄️ /nix/store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1
❯ cd conf

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/conf
❯ ls -la
total 64
dr-xr-xr-x  2 root root  4096 Jan  1  1970 .
dr-xr-xr-x 10 root root  4096 Jan  1  1970 ..
-r--r--r--  3 root root 14428 Jan  1  1970 config.yaml
-r--r--r--  4 root root  2940 Jan  1  1970 log4j-cli.properties
-r--r--r--  4 root root  3207 Jan  1  1970 log4j-console.properties
-r--r--r--  4 root root  2731 Jan  1  1970 log4j.properties
-r--r--r--  4 root root  2064 Jan  1  1970 log4j-session.properties
-r--r--r--  4 root root  2884 Jan  1  1970 logback-console.xml
-r--r--r--  4 root root  1569 Jan  1  1970 logback-session.xml
-r--r--r--  4 root root  2333 Jan  1  1970 logback.xml
-r--r--r--  4 root root    15 Jan  1  1970 masters
-r--r--r--  4 root root    10 Jan  1  1970 workers
-r--r--r--  4 root root  1434 Jan  1  1970 zoo.cfg

❄️ store/bz578ix1lqrajd3bjn6k1ghzk1iig539-flink-1.19.1/conf
❯                                                                             on  main [!?]

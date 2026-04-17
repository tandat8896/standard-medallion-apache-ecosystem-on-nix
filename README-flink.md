cat > /mnt/user-data/outputs/README-FLINK.md << 'EOF'
# Flink Setup trên NixOS - Bug Fix Journal

## 1. Phát hiện vấn đề ban đầu

```bash
which spark-class
# /nix/store/f4wkin4nphkwcv9wsar8bim9c95xqqcv-python3-3.13.12-env/bin/spark-class
```

Phát hiện `pkgs.spark` trong flake không được dùng — toàn bộ spark command đến từ `ps.pyspark` trong `pythonEnv`. `pkgs.spark` bị bỏ phí.

---

## 2. Flink không có trong nixpkgs

```bash
nix eval --raw 'nixpkgs#apacheFlink' --extra-experimental-features 'nix-command flakes' 2>/dev/null
# (trống)
```

Flink không có package sẵn trong nixpkgs. Phải dùng `fetchurl` để download tarball từ Apache.

---

## 3. Download Flink tarball qua fetchurl

```bash
nix-prefetch-url https://archive.apache.org/dist/flink/flink-1.19.1/flink-1.19.1-bin-scala_2.12.tgz
# path is '/nix/store/pd1dnsn7lc5cgpda68lw5xm02v0s0ykk-flink-1.19.1-bin-scala_2.12.tgz'
# 1kmxk7kkkmr6f88m0a7yxp0zfdxii6sr4gzh68cx8wfaxzrnjqar
```

Thêm vào flake.nix:

```nix
flink = pkgs.stdenv.mkDerivation rec {
  pname = "flink";
  version = "1.19.1";
  src = pkgs.fetchurl {
    url = "https://archive.apache.org/dist/flink/flink-${version}/flink-${version}-bin-scala_2.12.tgz";
    sha256 = "1kmxk7kkkmr6f88m0a7yxp0zfdxii6sr4gzh68cx8wfaxzrnjqar";
  };
  sourceRoot = ".";
  installPhase = ''
    mkdir -p $out
    cp -r flink-${version}/. $out/
    cp ${flinkKafkaJar} $out/lib/flink-connector-kafka-3.2.0-1.19.jar
  '';
};
```

---

## 4. Lỗi log Read-only file system

```
FileNotFoundException: /nix/store/.../flink-1.19.1/log/... (Read-only file system)
```

**Nguyên nhân:** Flink cố ghi log vào `/nix/store` — read-only trên NixOS.

**Fix:** Export `FLINK_LOG_DIR` ra ngoài project:

```nix
# shellHook trong flake.nix
export FLINK_LOG_DIR="$PWD/ETL/infrastructure/flink-data/logs"
mkdir -p $FLINK_LOG_DIR
```

---

## 5. Flink Kafka Connector thiếu

```bash
ls $FLINK_HOME/lib/ | grep kafka
# (trống)
```

**Fix:** Fetch jar từ Maven và bake vào Flink derivation:

```bash
nix-prefetch-url https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar
# 1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06
```

```nix
flinkKafkaJar = pkgs.fetchurl {
  url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar";
  sha256 = "1s2gq2r127xsxy3zyn4b5z21bsigzir1mg46kj6ayf19ld16rr06";
};
```

---

## 6. start-cluster.sh báo "No masters file"

```
No masters file. Please specify masters in 'conf/masters'.
```

**Fix:**

```bash
echo "localhost" > infrastructure/flink-data/conf/masters
```

---

## 7. start-cluster.sh báo "0 masters" — HA mode bật nhưng ZooKeeper chưa chạy

```
Starting HA cluster with 0 masters.
```

**Nguyên nhân:** `config.yaml` có `high-availability.type: zookeeper` nhưng ZooKeeper chưa start.

**Fix:** Start ZooKeeper trước, sau đó start Flink.

---

## 8. ZooKeeper config Read-only

ZooKeeper nix store read-only — không sửa được config mặc định.

**Fix:** Copy config ra ngoài project:

```bash
mkdir -p infrastructure/zookeeper-data/{conf,data,logs}
install -m 644 $ZOOCFGDIR_DEFAULT/zoo_sample.cfg infrastructure/zookeeper-data/conf/zoo.cfg
```

Export trong shellHook:

```nix
export ZOOCFGDIR="$PWD/ETL/infrastructure/zookeeper-data/conf"
export ZOO_LOG_DIR="$PWD/ETL/infrastructure/zookeeper-data/logs"
```

---

## 9. ZooKeeper cần Kerberos — setup KDC

Flink HA với ZooKeeper yêu cầu Kerberos authentication.

### 9.1 Kiểm tra krb5 trong nixpkgs

```bash
nix eval --raw 'nixpkgs#krb5' --extra-experimental-features 'nix-command flakes' 2>/dev/null
# /nix/store/cwmz3yfb7kqafg8ssd3jgypgbml41wlm-krb5-1.22.1
```

Thêm vào flake: `pkgs.krb5`

### 9.2 Tạo KDC database

```bash
# KDC config
mkdir -p infrastructure/kerberos-data/{conf,data,logs}

# krb5.conf (từ man page mẫu)
cat > infrastructure/kerberos-data/conf/krb5.conf << 'EOF'
[libdefaults]
    default_realm = ETL.LOCAL
    dns_lookup_kdc = false
    dns_lookup_realm = false
    renew_lifetime = 7d
    forwardable = true
    udp_preference_limit = 1

[realms]
    ETL.LOCAL = {
        kdc = localhost:8888
        admin_server = localhost
    }

[domain_realm]
    localhost = ETL.LOCAL
EOF

# kdc.conf
cat > infrastructure/kerberos-data/conf/kdc.conf << 'EOF'
[kdcdefaults]
    kdc_listen = 8888
    kdc_tcp_listen = 8888

[realms]
    ETL.LOCAL = {
        database_name = /home/tandat8896-nix/tandat-interview/ETL/infrastructure/kerberos-data/data/principal
        key_stash_file = /home/tandat8896-nix/tandat-interview/ETL/infrastructure/kerberos-data/data/.k5.ETL.LOCAL
    }
EOF

export KRB5_CONFIG="$PWD/infrastructure/kerberos-data/conf/krb5.conf"
export KRB5_KDC_PROFILE="$PWD/infrastructure/kerberos-data/conf/kdc.conf"
```

### 9.3 Tạo database

```bash
kdb5_util -d infrastructure/kerberos-data/data/principal \
  -sf infrastructure/kerberos-data/data/.k5.ETL.LOCAL \
  create -s -r ETL.LOCAL
```

**Lỗi:** Database đã tồn tại từ lần thử trước

```bash
rm -f infrastructure/kerberos-data/data/principal*
# Chạy lại lệnh trên
```

### 9.4 Tạo principals

```bash
# Policy
kadmin.local -d infrastructure/kerberos-data/data/principal -r ETL.LOCAL \
  -q "addpol -minlength 8 -minclasses 2 default"

# Flink principal
kadmin.local -d infrastructure/kerberos-data/data/principal -r ETL.LOCAL \
  -q "addprinc -randkey flink/localhost@ETL.LOCAL"

# ZooKeeper principal  
kadmin.local -d infrastructure/kerberos-data/data/principal -r ETL.LOCAL \
  -q "addprinc -randkey zookeeper/localhost@ETL.LOCAL"
```

### 9.5 Export keytabs

```bash
kadmin.local -d infrastructure/kerberos-data/data/principal -r ETL.LOCAL \
  -q "ktadd -k infrastructure/kerberos-data/data/flink.keytab flink/localhost@ETL.LOCAL"

kadmin.local -d infrastructure/kerberos-data/data/principal -r ETL.LOCAL \
  -q "ktadd -k infrastructure/kerberos-data/data/zookeeper.keytab zookeeper/localhost@ETL.LOCAL"
```

### 9.6 Test kinit

```bash
kinit -k -t infrastructure/kerberos-data/data/flink.keytab flink/localhost@ETL.LOCAL
klist
# Ticket cache: FILE:/tmp/krb5cc_1000
# Default principal: flink/localhost@ETL.LOCAL
# Valid starting: 04/18/2026 01:27:41  Expires: 04/19/2026 01:27:41
```

---

## 10. KDC port 88 bị block — cần root

```
bind: EACCES (Permission denied) port 88
```

**Fix:** Dùng port 8888

```bash
krb5kdc -n -r ETL.LOCAL -d infrastructure/kerberos-data/data/principal -p 8888 &
```

---

## 11. ZooKeeper SASL Authentication Failed

```
LoginException: Cannot locate KDC
```

**Nguyên nhân:** ZooKeeper không biết KDC ở đâu — cần trỏ `krb5.conf` qua JVM flag.

**Fix:** Tạo `java.env`:

```bash
cat > infrastructure/zookeeper-data/conf/java.env << 'EOF'
export SERVER_JVMFLAGS="-Djava.security.auth.login.config=/home/tandat8896-nix/tandat-interview/ETL/infrastructure/zookeeper-data/conf/zk_jaas.conf -Djava.security.krb5.conf=/home/tandat8896-nix/tandat-interview/ETL/infrastructure/kerberos-data/conf/krb5.conf"
EOF
```

Tạo `zk_jaas.conf`:

```bash
cat > infrastructure/zookeeper-data/conf/zk_jaas.conf << 'EOF'
Server {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/home/tandat8896-nix/tandat-interview/ETL/infrastructure/kerberos-data/data/zookeeper.keytab"
  storeKey=true
  useTicketCache=false
  principal="zookeeper/localhost@ETL.LOCAL";
};
EOF
```

---

## 12. Flink Authentication Failed với ZooKeeper

```
Authentication failed
```

**Fix:** Thêm `-Djava.security.krb5.conf` vào `env.java.opts.all` trong `config.yaml`:

```yaml
env:
  java:
    opts:
      all: ... -Djava.security.krb5.conf=/home/tandat8896-nix/tandat-interview/ETL/infrastructure/kerberos-data/conf/krb5.conf
```

---

## 13. Flink WebUI không vào được — random port

```
--webui-port 0
```

**Nguyên nhân:** `WEBUIPORT` env var không được set, script dùng port 0 (random).

**Fix:**

```bash
export WEBUIPORT=8081
```

Thêm vào shellHook flake.nix.

---

## Thứ tự khởi động đúng

```bash
# 1. Start KDC
krb5kdc -n -r ETL.LOCAL -d infrastructure/kerberos-data/data/principal -p 8888 &

# 2. Start ZooKeeper
zkServer.sh start

# 3. Start Flink
export WEBUIPORT=8081
start-cluster.sh
```

---

## Kiến trúc bảo mật

```
KDC (ETL.LOCAL realm, port 8888)
    ↓ Kerberos ticket
ZooKeeper (SASL/Kerberos, port 2181)
    ↓ HA leader election
Flink JobManager (keytab auth, UI port 8081)
    ↓
Flink TaskManager
    ↓
Kafka (port 9092/9094)
```

---

## Bài học rút ra

1. **NixOS read-only store** — tất cả config phải copy ra ngoài project
2. **Port < 1024 cần root** — KDC, ZooKeeper dùng port cao hơn
3. **JAAS config không bundle sẵn** — phải tự tạo, cấu trúc từ Apache docs
4. **`WEBUIPORT` env var** — kiểm soát Flink WebUI port, không phải chỉ config.yaml
5. **Thứ tự khởi động quan trọng** — KDC → ZooKeeper → Flink
EOF

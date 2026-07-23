# Locally Testing a Storage Backend End-to-End

Companion to [`adding-a-backend.md`](adding-a-backend.md). This walks
you through standing up a minimal test environment and exercising
INSERT / SELECT / JOIN against Iceberg tables backed by your backend.
Everything here is copy-paste runnable; no external test fixtures
needed.

## Scope

What this doc gives you:

- A minimal `docker-compose.yml` that brings up **HDFS (arm64-native)
  + MinIO** in ~30 seconds.
- Server-config templates (`s3.conf`, `gphdfs.conf`) with the exact
  keys the open-source dlagent expects.
- An end-to-end SQL script that creates, writes, and reads Iceberg
  tables on both backends.
- The four gotchas that cost the author hours on the first pass.

## Prerequisites

- A running gpdemo cluster (or any PG cluster where `datalake_fdw`
  is installed).
- Docker (Desktop on Mac, or `dockerd` on Linux).
- Network access to `docker.hashdata.dev` (internal registry) OR
  Docker Hub reachable for pulling `apache/hadoop`, `minio/minio`,
  and `apachehudi/hudi-hadoop_2.8.4-*` images.

On Apple Silicon the vanilla `apache/hadoop:3` image runs under qemu
and the JVM crashes with a safepoint invariant failure. **Use the
arm64-native `apachehudi/hudi-hadoop_2.8.4-*` images** shown below.

## 1. Bring up HDFS + MinIO

Create `~/datalake-verify/docker-compose.yml`:

```yaml
networks:
  dl-net:
    name: dl-net
    driver: bridge

volumes:
  nn1-data:
  dn1-data:
  minio-data:

services:
  nn1:
    image: docker.hashdata.dev/apachehudi/hudi-hadoop_2.8.4-namenode:linux-arm64-0.10.1
    hostname: nn1
    container_name: dl-nn1
    networks:
      dl-net:
        aliases: [nn1]
    ports:
      - "9870:50070"
      - "9000:8020"
    volumes:
      - nn1-data:/hadoop/dfs/name
    environment:
      CLUSTER_NAME: dl
      HDFS_CONF_dfs_namenode_name_dir: file:///hadoop/dfs/name
      CORE_CONF_fs_defaultFS: hdfs://nn1:8020
      CORE_CONF_hadoop_http_staticuser_user: root
      HDFS_CONF_dfs_permissions_enabled: "false"
      HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check: "false"
      HDFS_CONF_dfs_replication: "1"
      HDFS_CONF_dfs_client_use_datanode_hostname: "true"
      HDFS_CONF_dfs_datanode_use_datanode_hostname: "true"

  dn1:
    image: docker.hashdata.dev/apachehudi/hudi-hadoop_2.8.4-datanode:linux-arm64-0.10.1
    hostname: dn1
    container_name: dl-dn1
    networks:
      dl-net:
        aliases: [dn1]
    volumes:
      - dn1-data:/hadoop/dfs/data
    environment:
      CLUSTER_NAME: dl
      SERVICE_PRECONDITION: "nn1:8020"
      HDFS_CONF_dfs_datanode_data_dir: file:///hadoop/dfs/data
      CORE_CONF_fs_defaultFS: hdfs://nn1:8020
      HDFS_CONF_dfs_permissions_enabled: "false"
      HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check: "false"
      HDFS_CONF_dfs_client_use_datanode_hostname: "true"
      HDFS_CONF_dfs_datanode_use_datanode_hostname: "true"
    depends_on: [nn1]

  minio:
    image: docker.hashdata.dev/minio/minio:RELEASE.2024-01-01T16-36-33Z
    hostname: minio
    container_name: dl-minio
    networks:
      dl-net:
        aliases: [minio]
    ports:
      - "9100:9000"
      - "9101:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    command: server /data --console-address ":9001"
```

Bring up:

```bash
cd ~/datalake-verify
docker compose up -d

# Wait for NameNode to exit safe mode (~15 seconds)
until docker exec dl-nn1 hdfs dfsadmin -safemode get 2>&1 | grep -q "Safe mode is OFF"; do sleep 3; done

# Create warehouse dir in HDFS
docker exec dl-nn1 hdfs dfs -mkdir -p /warehouse/iceberg
docker exec dl-nn1 hdfs dfs -chmod -R 777 /warehouse

# Create MinIO bucket
docker exec dl-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec dl-minio mc mb -p local/iceberg
```

## 2. Attach the dev container to `dl-net`

`datalake_fdw` running inside your existing dev container needs to
resolve `nn1`, `dn1`, `minio` by hostname.

```bash
# Replace <dev-container> with your container name
docker network connect dl-net <dev-container>

# Verify
docker exec <dev-container> getent hosts nn1 dn1 minio
```

## 3. Write server-config files

The Java `dlagent` reads per-server YAML at query time. Place these
in the coordinator data dir **and every segment data dir** (dlagent's
`FileInputStream` uses the process CWD, which is the data dir).

### `s3.conf` (for S3 backend)

```yaml
s3_dev:
    gopher.enabled: 'false'
    fs.defaultFS: "s3a://"
    fs.s3a.endpoint: "http://minio:9000"
    fs.s3a.access.key: minioadmin
    fs.s3a.secret.key: minioadmin
    fs.s3a.path.style.access: 'true'
    fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
    fs.s3a.connection.ssl.enabled: 'false'
```

Where `s3_dev` matches the `server_name` option on your foreign table.

### `gphdfs.conf` (for HDFS backend)

```yaml
hdfs_dev:
    gopher.enabled: 'false'
    hdfs_namenode_host: nn1
    hdfs_namenode_port: '8020'
    hdfs_auth_method: simple
    dfs.client.use.datanode.hostname: 'true'
```

**Note:** use the container-internal port `8020`, not the host-mapped
`9000`. From the dev container (which is on `dl-net`) you reach NN
through the internal port.

### Copy to all data dirs

```bash
for d in $COORDINATOR_DATA_DIRECTORY \
         $COORDINATOR_DATA_DIRECTORY/../../standby \
         $COORDINATOR_DATA_DIRECTORY/../../dbfast1/demoDataDir0 \
         $COORDINATOR_DATA_DIRECTORY/../../dbfast2/demoDataDir1 \
         $COORDINATOR_DATA_DIRECTORY/../../dbfast3/demoDataDir2; do
  cp /tmp/s3.conf $d/s3.conf
  cp /tmp/gphdfs.conf $d/gphdfs.conf
done
```

## 4. Disable dlagent's Gopher default

The `dlagent-1.0.0.jar` has `gopher.enabled=true` baked into
`application.properties`. The open-source build can't reach a Gopher
daemon, so **every backend initialization will fail** unless you
override it. Two places:

- **JVM startup** (master switch): set before `gpstart`:
  ```bash
  export JAVA_TOOL_OPTIONS='-Dgopher.enabled=false'
  gpstart -a
  ```
- **Per-server config** (belt-and-suspenders): the `gopher.enabled: 'false'`
  line already in `s3.conf` / `gphdfs.conf` above.

## 5. Run the end-to-end SQL test

```bash
psql -d postgres -p $PGPORT <<'SQL'
-- Using the Postgres planner avoids an orthogonal ORCA SIGSEGV on
-- foreign-table queries. Not a registry-framework issue.
SET optimizer = off;

DROP EXTENSION IF EXISTS datalake_fdw CASCADE;
CREATE EXTENSION datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');

---- S3 side ----
CREATE SERVER s3_srv FOREIGN DATA WRAPPER datalake_fdw
  OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR PUBLIC SERVER s3_srv
  OPTIONS (accesskey 'minioadmin', secretkey 'minioadmin');
CREATE FOREIGN TABLE s3_ice (a int, b text) SERVER s3_srv
  OPTIONS (filepath '/iceberg/', catalog_type 's3',
           server_name 's3_dev', table_identifier 'test.basic_v1',
           format 'iceberg');

---- HDFS side ----
CREATE SERVER hdfs_srv FOREIGN DATA WRAPPER datalake_fdw
  OPTIONS (protocol 'hdfs', hdfs_namenodes 'nn1:8020', hdfs_auth_method 'simple');
CREATE USER MAPPING FOR PUBLIC SERVER hdfs_srv
  OPTIONS (user 'root', password 'dummy');
CREATE FOREIGN TABLE hdfs_ice (a int, b text) SERVER hdfs_srv
  OPTIONS (filepath '/warehouse/iceberg/', catalog_type 'hadoop',
           server_name 'hdfs_dev', table_identifier 'default.hdfs_test_v1',
           format 'iceberg');

---- Writes ----
INSERT INTO s3_ice   VALUES (1, 'hello'), (2, 'world');
INSERT INTO hdfs_ice VALUES (1, 'hdfs_one'), (2, 'hdfs_two');

---- Reads ----
SELECT * FROM s3_ice   ORDER BY a;
SELECT * FROM hdfs_ice ORDER BY a;

---- Cross-backend JOIN (proves independent dispatch) ----
SELECT h.a, h.b AS hdfs_b, s.b AS s3_b
  FROM hdfs_ice h JOIN s3_ice s ON h.a = s.a
  ORDER BY h.a;
SQL
```

Expected output (last query):

```
 a | hdfs_b   | s3_b
---+----------+-------
 1 | hdfs_one | hello
 2 | hdfs_two | world
```

## 6. Inspect persisted files

```bash
# HDFS
docker exec dl-nn1 hdfs dfs -ls -R /warehouse/iceberg/default/hdfs_test_v1
# Expect: data/*.parquet and metadata/{v1,v2}.metadata.json,
#         snap-*.avro, *-m0.avro

# MinIO
docker exec dl-minio mc ls -r local/iceberg
# Expect: test/basic_v1/data/*.parquet and test/basic_v1/metadata/*
```

## 7. Cleanup

```bash
cd ~/datalake-verify
docker compose down -v          # removes containers + volumes
docker network rm dl-net 2>/dev/null || true
```

## Gotchas (in the order you will hit them)

1. **Apple Silicon + apache/hadoop:3** — qemu emulation crashes the JVM.
   Use `apachehudi/hudi-hadoop_2.8.4-*:linux-arm64-0.10.1` (native arm64).
2. **Dev container can't reach `nn1`/`minio`** — forgot to `docker
   network connect dl-net <dev-container>`.
3. **HDFS port mismatch** — inside `dl-net` namenode listens on `8020`
   (internal), not `9000` (host-mapped). Your SQL and `gphdfs.conf`
   must both use `8020`.
4. **dlagent keeps saying "Failed to initialize GopherFileIO"** — you
   forgot `JAVA_TOOL_OPTIONS=-Dgopher.enabled=false` before `gpstart`,
   or your `.conf` file lacks `gopher.enabled: 'false'`. You need
   **both** lines.
5. **INSERT succeeds but SELECT crashes coordinator** — unrelated ORCA
   bug on foreign tables. `SET optimizer = off` bypasses it. File a
   separate issue against ORCA; not a backend problem.
6. **`No such file or directory: s3a://iceberg/.../version-hint.text`**
   on first INSERT — expected for a new Iceberg table; the INSERT
   writes it. Run the INSERT and SELECT again.

## Adapting to a new backend

To validate your Azure / GCS / WebHDFS / … backend:

1. Replace the container for your service (e.g., `mcr.microsoft.com/azure-storage/azurite`).
2. Write a per-server config file (analogous to `s3.conf`) with the
   keys your backend reads inside `createHandle()`.
3. Replace the `CREATE SERVER … protocol 'myproto'` line with your
   registered protocol name.
4. The rest of the SQL (INSERT / SELECT / JOIN) is backend-agnostic —
   if those succeed, your backend's `openFile`/`read`/`write`/`listInfo`
   contracts all hold.

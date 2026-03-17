# trino-ducklake

A [Trino](https://trino.io/) connector for [DuckLake](https://ducklake.select/). **Work in progress.**

## Build

```bash
./mvnw clean package -DskipTests
```

## Run

Download and extract [Trino](https://trino.io/download.html):

```bash
curl -O https://github.com/trinodb/trino/releases/download/479/trino-server-479.tar.gz
tar xzf trino-server-479.tar.gz
```

Install the plugin:

```bash
cp -r target/ducklake trino-server-479/plugin/ducklake
```

Create `trino-server-479/etc/catalog/ducklake.properties`:

```properties
connector.name=ducklake
ducklake.metadata-connection-string=jdbc:sqlite:/absolute/path/to/metadata.sqlite
fs.native-local.enabled=true
local.location=/
```

`data_path` is read from `ducklake_metadata` inside the catalog metadata database. It is not configured as a connector property.

### S3

```properties
connector.name=ducklake
ducklake.metadata-connection-string=jdbc:postgresql://host/db
fs.native-s3.enabled=true
s3.region=us-east-1
```

Authentication uses Trino's standard S3 filesystem configuration (IAM roles, access keys, etc.).

### GCS

```properties
connector.name=ducklake
ducklake.metadata-connection-string=jdbc:postgresql://host/db
fs.native-gcs.enabled=true
```

Authentication uses Trino's standard GCS filesystem configuration (service accounts, etc.).

Start Trino:

```bash
trino-server-479/bin/launcher run
```

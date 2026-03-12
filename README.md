# trino-ducklake

A [Trino](https://trino.io/) connector for [DuckLake](https://ducklake.select/). **Work in progress.**

## Build

```bash
./mvnw clean package -DskipTests
```

## Run

Download and extract [Trino](https://trino.io/download.html):

```bash
curl -O https://repo1.maven.org/maven2/io/trino/trino-server/479/trino-server-479.tar.gz
tar xzf trino-server-479.tar.gz
```

Install the plugin:

```bash
cp -r target/ducklake trino-server-479/plugin/ducklake
```

Create `trino-server-479/etc/catalog/ducklake.properties`:

```properties
connector.name=ducklake
ducklake.metadata-connection-string=postgres:dbname=ducklake_catalog host=localhost
ducklake.data-path=s3://my-bucket/data/
```

Start Trino:

```bash
trino-server-479/bin/launcher run
```

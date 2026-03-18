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

## Docker Compose

The included `docker-compose.yml` provides ready-to-use profiles for different storage backends.

### SQLite (local files)

```bash
docker compose --profile sqlite up
```

Mounts `metadata.sqlite` and `data_files/` from the project directory. Generate test data first with `./generate-ducklake-database.sh`.

### PostgreSQL (local files)

```bash
docker compose --profile postgres up
```

Starts Trino with a PostgreSQL sidecar for metadata storage. Data files are mounted from `data_files_pg/`.

### MinIO (S3-compatible object storage)

```bash
docker compose --profile minio up
```

Starts Trino + PostgreSQL + MinIO. The `ducklake` bucket is created automatically.

- Trino: `http://localhost:8080`
- MinIO Console: `http://localhost:9001` (login: `minioadmin` / `minioadmin`)
- MinIO API: `http://localhost:9000`

### Environment variables

The Docker image is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `METADATA_CONNECTION_STRING` | JDBC URL for the metadata database | `jdbc:sqlite:/data/metadata.sqlite` (local) or `jdbc:postgresql://postgres:5432/ducklake_meta?...` (s3/gcs) |
| `FS_MODE` | Filesystem mode: `local`, `s3`, or `gcs` | `local` |
| `S3_REGION` | AWS region (for `s3` mode) | `us-east-1` |
| `S3_ENDPOINT` | Custom S3 endpoint (for MinIO or other S3-compatible stores) | *(none)* |

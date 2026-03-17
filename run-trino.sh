#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 [sqlite|postgres|s3|gcs]"
    echo ""
    echo "Build the connector, install it into a local Trino, and start Trino."
    echo ""
    echo "Modes:"
    echo "  sqlite    (default) Use metadata.sqlite as the metadata database"
    echo "  postgres  Use PostgreSQL on localhost:5433 as the metadata database"
    echo "            Requires: docker container 'ducklake-test-pg' on port 5433"
    echo "  s3        Use PostgreSQL metadata + S3 data files"
    echo "            Env vars: METADATA_CONN (default: postgres on localhost:5433)"
    echo "                      S3_REGION (default: us-east-1)"
    echo "  gcs       Use PostgreSQL metadata + GCS data files"
    echo "            Env vars: METADATA_CONN (default: postgres on localhost:5433)"
    exit 1
}

MODE="${1:-sqlite}"

case "$MODE" in
    sqlite)
        METADATA_CONN="jdbc:sqlite:$(pwd)/metadata.sqlite"
        ;;
    postgres)
        METADATA_CONN="jdbc:postgresql://localhost:5433/ducklake_meta?user=postgres&password=testpass"
        ;;
    s3)
        METADATA_CONN="${METADATA_CONN:-jdbc:postgresql://localhost:5433/ducklake_meta_s3?user=postgres&password=testpass}"
        S3_REGION="${S3_REGION:-us-east-1}"
        ;;
    gcs)
        METADATA_CONN="${METADATA_CONN:-jdbc:postgresql://localhost:5433/ducklake_meta_gcs?user=postgres&password=testpass}"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown mode: $MODE"
        usage
        ;;
esac

TRINO_VERSION=479
TRINO_DIR="trino-server-${TRINO_VERSION}"
TRINO_TARBALL="${TRINO_DIR}.tar.gz"
DATA_DIR="$(pwd)/.trino-data"

# Build the plugin
echo "Building plugin..."
./mvnw clean package -DskipTests -q

# Download Trino if needed
if [ ! -d "${TRINO_DIR}" ]; then
    echo "Downloading Trino ${TRINO_VERSION}..."
    curl -fSL -O "https://github.com/trinodb/trino/releases/download/${TRINO_VERSION}/${TRINO_TARBALL}"
    tar xzf "${TRINO_TARBALL}"
    rm "${TRINO_TARBALL}"
fi

# Install plugin
rm -rf "${TRINO_DIR}/plugin/ducklake"
cp -r target/ducklake "${TRINO_DIR}/plugin/ducklake"

# Create data directory
mkdir -p "${DATA_DIR}"

# Write minimal single-node config
mkdir -p "${TRINO_DIR}/etc/catalog"

cat > "${TRINO_DIR}/etc/node.properties" <<EOF
node.environment=dev
node.data-dir=${DATA_DIR}
EOF

cat > "${TRINO_DIR}/etc/jvm.config" <<EOF
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
--enable-native-access=ALL-UNNAMED
EOF

cat > "${TRINO_DIR}/etc/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF

if [ "$MODE" = "s3" ]; then
    cat > "${TRINO_DIR}/etc/catalog/ducklake.properties" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONN}
fs.native-s3.enabled=true
s3.region=${S3_REGION}
EOF
elif [ "$MODE" = "gcs" ]; then
    cat > "${TRINO_DIR}/etc/catalog/ducklake.properties" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONN}
fs.native-gcs.enabled=true
EOF
else
    cat > "${TRINO_DIR}/etc/catalog/ducklake.properties" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONN}
fs.native-local.enabled=true
local.location=/
EOF
fi

echo "Starting Trino on http://localhost:8080 (mode: ${MODE})"
exec "${TRINO_DIR}/bin/launcher" run

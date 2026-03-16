#!/usr/bin/env bash
set -euo pipefail

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

cat > "${TRINO_DIR}/etc/catalog/ducklake.properties" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=jdbc:sqlite:$(pwd)/metadata.sqlite
EOF

echo "Starting Trino on http://localhost:8080"
exec "${TRINO_DIR}/bin/launcher" run

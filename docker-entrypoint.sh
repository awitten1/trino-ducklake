#!/usr/bin/env bash
set -euo pipefail

CATALOG_DIR="/etc/trino/catalog"
CATALOG_FILE="${CATALOG_DIR}/ducklake.properties"

if [ ! -f "${CATALOG_FILE}" ]; then
    mkdir -p "${CATALOG_DIR}"

    FS_MODE="${FS_MODE:-local}"

    if [ -z "${METADATA_CONNECTION_STRING:-}" ]; then
        case "$FS_MODE" in
            s3|gcs)
                METADATA_CONNECTION_STRING="jdbc:postgresql://postgres:5432/ducklake_meta?user=postgres&password=testpass"
                ;;
            local|*)
                METADATA_CONNECTION_STRING="jdbc:sqlite:/data/metadata.sqlite"
                ;;
        esac
    fi

    case "$FS_MODE" in
        s3)
            cat > "${CATALOG_FILE}" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONNECTION_STRING}
fs.native-s3.enabled=true
s3.region=${S3_REGION:-us-east-1}
EOF

            if [ -n "${S3_ENDPOINT:-}" ]; then
                cat >> "${CATALOG_FILE}" <<EOF
s3.endpoint=${S3_ENDPOINT}
s3.path-style-access=true
EOF
            fi
            ;;
        gcs)
            cat > "${CATALOG_FILE}" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONNECTION_STRING}
fs.native-gcs.enabled=true
EOF
            ;;
        local|*)
            cat > "${CATALOG_FILE}" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONNECTION_STRING}
fs.native-local.enabled=true
local.location=/
EOF
            ;;
    esac
fi

exec /usr/lib/trino/bin/run-trino "$@"

#!/usr/bin/env bash
set -euo pipefail

CATALOG_DIR="/etc/trino/catalog"
CATALOG_FILE="${CATALOG_DIR}/ducklake.properties"

if [ ! -f "${CATALOG_FILE}" ]; then
    mkdir -p "${CATALOG_DIR}"

    FS_MODE="${FS_MODE:-local}"

    if [ -z "${METADATA_CONNECTION_STRING:-}" ]; then
        echo "ERROR: METADATA_CONNECTION_STRING is required (e.g. jdbc:sqlite:/data/metadata.sqlite or jdbc:postgresql://host:5432/db?user=...)" >&2
        exit 1
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
                echo "s3.endpoint=${S3_ENDPOINT}" >> "${CATALOG_FILE}"
            fi
            if [ "${S3_PATH_STYLE_ACCESS:-false}" = "true" ]; then
                echo "s3.path-style-access=true" >> "${CATALOG_FILE}"
            fi
            ;;
        gcs)
            cat > "${CATALOG_FILE}" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONNECTION_STRING}
fs.native-gcs.enabled=true
EOF
            ;;
        azure)
            cat > "${CATALOG_FILE}" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONNECTION_STRING}
fs.native-azure.enabled=true
EOF
            ;;
        local)
            cat > "${CATALOG_FILE}" <<EOF
connector.name=ducklake
ducklake.metadata-connection-string=${METADATA_CONNECTION_STRING}
fs.native-local.enabled=true
local.location=/data
EOF
            ;;
        *)
            echo "ERROR: Unknown FS_MODE '$FS_MODE'. Supported: local, s3, gcs, azure" >&2
            exit 1
            ;;
    esac
fi

exec /usr/lib/trino/bin/run-trino "$@"

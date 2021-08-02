#!/usr/bin/env bash
set -euxo pipefail

GOGOPROTO_IMPORT="protobuf-import"
IMPORT_PACKAGE_DIR="github.com/gogo"
IMPORT_PACKAGE="${IMPORT_PACKAGE_DIR}/protobuf"

function cleanup {
  rm -rf "${GOGOPROTO_IMPORT}"
}

cleanup
trap cleanup EXIT

mkdir -p "${GOGOPROTO_IMPORT}/${IMPORT_PACKAGE_DIR}"

go list -f "{{ .Dir }} ${GOGOPROTO_IMPORT}/{{ .Path }}" -m ${IMPORT_PACKAGE} \
  | xargs -L1 -- ln -s

protoc --gofast_out=plugins=grpc:. -I=. -I=${GOGOPROTO_IMPORT} ./proto/lancepb/*
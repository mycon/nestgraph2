#!/usr/bin/env bash
# compile_protos.sh — compile Nest protobuf definitions to Python bindings.
#
# Prerequisites (Linux / macOS / WSL):
#   pip install grpcio-tools
#   (or: apt install protobuf-compiler && pip install grpcio-tools)
#
# Usage:
#   bash scripts/compile_protos.sh
#
# Output: proto/generated/  (Python *_pb2.py files)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_ROOT="$REPO_ROOT/proto"
OUT_DIR="$PROTO_ROOT/generated"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/__init__.py"

echo "=== Compiling protos from $PROTO_ROOT and $REPO_ROOT/nest-observe/protobuf/pb ==="

# Find all .proto files under proto/ and nest-observe/protobuf/pb
PROTO_FILES=$(find "$PROTO_ROOT" "$REPO_ROOT/nest-observe/protobuf/pb" -name "*.proto" ! -path "*/generated/*")

if [ -z "$PROTO_FILES" ]; then
    echo "No .proto files found under $PROTO_ROOT or $REPO_ROOT/nest-observe/protobuf/pb"
    exit 1
fi

python -m grpc_tools.protoc \
    -I"$PROTO_ROOT" \
    -I"$REPO_ROOT/nest-observe/protobuf/pb" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    $PROTO_FILES

echo "=== Compilation complete ==="
echo "Generated files:"
find "$OUT_DIR" -name "*.py" ! -name "__init__.py" | sort

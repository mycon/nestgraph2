#!/usr/bin/env python3
"""
Compile all .proto files under proto/ and nest-observe/protobuf/pb/ to Python bindings.
More reliable than the .bat file on Windows because it avoids shell quoting issues.
"""
import os
import sys
import subprocess
from pathlib import Path

REPO = Path(__file__).parent.parent.resolve()
PROTO_ROOTS = [
    REPO / "proto",
    REPO / "nest-observe" / "protobuf" / "pb",
]
OUT_DIR = REPO / "proto" / "generated"

OUT_DIR.mkdir(parents=True, exist_ok=True)
init = OUT_DIR / "__init__.py"
if not init.exists():
    init.touch()

proto_files = []
for root in PROTO_ROOTS:
    for p in root.rglob("*.proto"):
        if "generated" not in p.parts:
            proto_files.append(str(p))

print(f"Found {len(proto_files)} .proto files")

proto_path_args = []
for root in PROTO_ROOTS:
    proto_path_args += [f"--proto_path={root}"]

cmd = [
    sys.executable, "-m", "grpc_tools.protoc",
    *proto_path_args,
    f"--python_out={OUT_DIR}",
    *proto_files,
]

print("Running protoc...")
result = subprocess.run(cmd, capture_output=True, text=True)

if result.stdout:
    print(result.stdout)
if result.stderr:
    print(result.stderr, file=sys.stderr)

if result.returncode == 0:
    generated = sorted(OUT_DIR.rglob("*_pb2.py"))
    print(f"\n=== Compilation successful — {len(generated)} files generated ===")
    for f in generated:
        print(f"  {f.relative_to(OUT_DIR)}")
else:
    print(f"\n=== Compilation FAILED (exit {result.returncode}) ===", file=sys.stderr)
    sys.exit(result.returncode)

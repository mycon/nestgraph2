# Protobuf Compilation for nestgraph2

This document describes how to compile the Nest protobuf definitions and install the generated Python bindings into `proto/generated/`.

## Prerequisites

- Python 3 installed
- `grpcio-tools` available in your Python environment

## Compile the protobufs

From the `nestgraph2` repository root:

### Linux / macOS / WSL

```bash
pip install grpcio-tools
bash scripts/compile_protos.sh
```

### Windows

```bat
python -m pip install grpcio-tools
scripts\compile_protos.bat
```

## What is compiled

The script compiles all `.proto` files found under:

- `proto/`
- `nest-observe/protobuf/pb/`

and places the generated Python modules into:

- `proto/generated/`

## Expected output

After running the compiler, `proto/generated/` should contain files such as:

- `nest_gateway_pb2.py`
- `nest/trait/hvac_extra_pb2.py`
- `nest/trait/service_extra_pb2.py`
- `nestlabs/gateway/v1_pb2.py`
- `nest/trait/hvac_pb2.py`
- `nest/trait/sensor_pb2.py`

## Notes

- The generated files are used by `src/grpc_web.py` and other Python code that parses Nest Observe responses.
- If the compilation fails, make sure `grpcio-tools` is installed in the same Python environment you are using.

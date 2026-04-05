@echo off
REM compile_protos.bat — compile Nest protobuf definitions to Python bindings (Windows).
REM
REM Prerequisites:
REM   pip install grpcio-tools
REM
REM Usage:
REM   scripts\compile_protos.bat
REM
REM Output: proto\generated\  (Python *_pb2.py files)

setlocal EnableDelayedExpansion

SET REPO_ROOT=%~dp0..
SET PROTO_ROOT=%REPO_ROOT%\proto
SET OUT_DIR=%PROTO_ROOT%\generated

if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"
if not exist "%OUT_DIR%\__init__.py" type nul > "%OUT_DIR%\__init__.py"

echo === Compiling protos from %PROTO_ROOT% and %REPO_ROOT%\nest-observe\protobuf\pb ===

REM Build a space-separated list of .proto files
SET PROTO_FILES=
for /R "%PROTO_ROOT%" %%F in (*.proto) do (
    echo %%F | findstr /V "\\generated\\" > nul && SET "PROTO_FILES=!PROTO_FILES! %%F"
)
for /R "%REPO_ROOT%\nest-observe\protobuf\pb" %%F in (*.proto) do (
    echo %%F | findstr /V "\\generated\\" > nul && SET "PROTO_FILES=!PROTO_FILES! %%F"
)

if "!PROTO_FILES!"=="" (
    echo No .proto files found under %PROTO_ROOT% or %REPO_ROOT%\nest-observe\protobuf\pb
    exit /b 1
)

python -m grpc_tools.protoc ^
    -I"%PROTO_ROOT%" ^
    -I"%REPO_ROOT%\nest-observe\protobuf\pb" ^
    --python_out="%OUT_DIR%" ^
    --grpc_python_out="%OUT_DIR%" ^
    !PROTO_FILES!

echo === Compilation complete ===
echo Generated files:
dir /B "%OUT_DIR%\*.py" 2>nul | findstr /V "__init__"

endlocal

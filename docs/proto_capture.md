# Capturing the Nest SDK JS Bundle for Proto Updates

The Nest web app ships a minified JavaScript bundle that contains all
protobuf message definitions as inline JS deserializer functions.  When
Nest deploys a new app version the bundle filename changes and new traits
may appear.  This document explains how to capture a fresh bundle and
how to regenerate the trait list and proto files.

---

## When to re-capture

- You see an unknown `type_url` in an Observe response (grep for `"unknown"` in
  `probe_grpc.py` output).
- The bundle filename in the request log changes (e.g. `sdk-XXXXXXXX.js`
  differs from the current `sdk-421f7ba4a82b319d562a.js`).
- A Nest firmware update adds new traits that you want to decode.

---

## Step 1 — Capture the JS bundle

1. Open **Chrome** and navigate to [home.nest.com](https://home.nest.com).
2. Open **DevTools** (F12) → **Network** tab.
3. Filter by `sdk-`.  You should see one request matching `sdk-*.js`.
   If the page is already loaded, reload it (F5) while the Network tab is
   open.
4. Right-click the matching request → **Save as…** and save to
   `tests/js_dump/sdk-<hash>.js`.
5. The `js_dump/` directory is gitignored so the file will not be
   committed.

---

## Step 2 — Scan for all trait names

```bash
python tests/scan_js.py tests/js_dump/sdk-<hash>.js
```

`scan_js.py` searches for every `proto.nest.trait.*Trait` occurrence and
writes `tests/fields_output.txt` with the full list.

To get a machine-readable list of just the trait names:

```python
import re
with open("tests/js_dump/sdk-<hash>.js") as f:
    text = f.read()
traits = sorted(set(re.findall(r"nest\.trait\.[\w\.]+Trait", text)))
for t in traits:
    print(t)
```

---

## Step 3 — Extract field maps for new / changed traits

```bash
python tests/extract_sdk.py tests/js_dump/sdk-<hash>.js HvacControlTrait FanControlTrait
```

The script searches for `deserializeBinaryFromReader` blocks and prints
the `switch(e.getFieldNumber())` cases, which map directly to protobuf
field numbers.

Alternatively, use `tests/scan_js.py` to dump the `serializeBinaryToWriter`
blocks, which are often cleaner.

---

## Step 4 — Update proto/nest_hvac_extra.proto

Edit `proto/nest_hvac_extra.proto` to:
- Add any new message types.
- Update field numbers / types for changed messages.

The file is hand-maintained and serves as the canonical proto spec for
traits not covered by the `nest-observe` submodule.

---

## Step 5 — Recompile Python bindings

```bash
# Linux / macOS / WSL:
bash scripts/compile_protos.sh

# Windows:
scripts\compile_protos.bat
```

Add `grpcio-tools` first if not already installed:

```bash
pip install grpcio-tools
```

---

## Step 6 — Update OBSERVE_TRAITS_THERMOSTAT

In `src/grpc_web.py`, the list `OBSERVE_TRAITS_THERMOSTAT` contains every
trait to subscribe to.  Add new trait names from Step 2 as needed.

---

## Capturing a SendCommand HAR trace

To verify / update the fan timer stub in `src/send_commands.py`:

1. Open Chrome DevTools → **Network** tab.
2. Filter by `SendCommand`.
3. In the Nest app, tap the fan icon and set a 15-minute timer.
4. Right-click the `SendCommand` request → **Copy** → **Copy as cURL**.
5. Paste into a text file.  The request body is the raw protobuf bytes
   (or base64-encoded — check the Content-Transfer-Encoding header).
6. Decode the body:
   ```python
   import base64, sys
   # If base64-encoded:
   data = base64.b64decode(open("body.txt").read().strip())
   # Then decode as proto:
   from src.grpc_web import decode_proto_fields
   for f, w, v in decode_proto_fields(data):
       print(f, w, repr(v))
   ```
7. Match the field numbers and `type_url` string to the stub in
   `src/send_commands.py` and update accordingly.

---

## SDK Bundle History

| Bundle filename                         | App version    | Date captured |
|-----------------------------------------|----------------|---------------|
| `sdk-421f7ba4a82b319d562a.js`           | 9.266.0        | 2026-04-06    |

Update this table every time you capture a new bundle.

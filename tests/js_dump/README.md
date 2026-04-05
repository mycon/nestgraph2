# Google Nest Web App JavaScript Resources

This directory (`tests/js_dump/`) contains copies of the Google Nest front-end JavaScript bundles downloaded from `home.nest.com`. These files are used as static references for reverse-engineering Nest's internal protobuf schemas, gRPC web endpoints, and session cookie validation schemas.

## Mapped Resources

The following table lists the active files and their corresponding upstream URLs:

| File Name | Upstream URL | Purpose / Notes |
| :--- | :--- | :--- |
| **`common-e754c34dcc5abc22ba35.js`** | `https://home.nest.com/js/_vendors_/common-e754c34dcc5abc22ba35.js` | Core third-party vendor dependencies (contains cookie setter helpers, WebGL, etc.). |
| **`main-36606966c8417ddf91d0.js`** | `https://home.nest.com/js/_shared_/main-36606966c8417ddf91d0.js` | Main shared application runtime bundle. |
| **`main-84209c0f2f2486fc3627.js`** | `https://home.nest.com/js/_vendors_/main-84209c0f2f2486fc3627.js` | Vendor-specific extension bundle. |
| **`sdk-421f7ba4a82b319d562a.js`** | `https://home.nest.com/js/_vendors_/lib/phoenix-sdk/sdk-421f7ba4a82b319d562a.js` | The Phoenix SDK client implementation (contains gRPC channel configurations, stream heartbeat management, and protobuf serializing logic). |
| **`thermostat-c75045b13e00ac156fa1.js`** | `https://home.nest.com/js/bundles/thermostat-c75045b13e00ac156fa1.js` | Thermostat state machine module (contains protobuf structure references for HVAC, fan, and temperature traits). |

---

## How to Redownload / Update

To redownload or restore these files to their exact versions, run the included PowerShell script:

1. Open PowerShell.
2. Execute the script:
   ```powershell
   powershell -ExecutionPolicy Bypass -File tests/js_dump/download_js_files.ps1
   ```

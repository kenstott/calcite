# Senate eFD fetch through a Windows-side Edge

`efdsearch.senate.gov` (Akamai) answers the agreement POST with HTTP 403 to every client on the
Linux/WSL side: the Java client, curl, headless and headed Chrome, with or without automation. A
Windows-side Edge is accepted. The `law` `fd_*` tables therefore run their session inside Edge:
`fetch-efd.js` starts the Windows `msedge.exe` with a fresh temporary profile and
`--remote-debugging-port` (localhost), drives it with `puppeteer-core` over the DevTools port,
clicks through the agreement like a person, then issues each listing/report request from inside
that page. The browser is closed (and its profile deleted) when the run ends. It is the headed,
unmodified Edge: no stealth plugin, no spoofed fingerprint, no cookie replay. No fallback exists:
if Edge, the helper or the port is unavailable the tables fail and the error names the cause.

`edge-session.js` (start/stop of a Windows Edge over CDP) and the Java `BrowserHelperProcess`
(line protocol over stdin/stdout) are the shared pieces for other browser-gated sources.

## Install (once per host)

    govdata/scripts/edge-browser/install.sh

Pins `puppeteer-core` 23.11.1 (no browser download) and prints the path to set as
`EFD_BROWSER_SCRIPT`. Needs `node` >= 18, `npm`, and WSL interop to `msedge.exe`.

## Settings (law schema operands; `${VAR:default}` in `law-schema.yaml`)

| Operand | Env var | Default |
|---|---|---|
| `efdBrowserScript` | `EFD_BROWSER_SCRIPT` | unset (required) |
| `efdEdgeExecutable` | `EFD_EDGE_EXECUTABLE` | `/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe` |
| `efdDebugPort` | `EFD_DEBUG_PORT` | 9222 |
| `efdProfileDir` | `EFD_PROFILE_DIR` | `C:\Users\Admin\AppData\Local\Temp` (a per-run `efd-<pid>-<time>` folder is created under it) |
| `efdNodeCommand` | `EFD_NODE_COMMAND` | `node` |

If the debug port already answers, the helper refuses to start rather than drive a browser it did
not launch.

# uscis.gov workbook fetch

uscis.gov (Akamai) answers HTTP 403 to every non-browser client and to headless Chrome. The
census `uscis_i765_employment_authorization` table therefore gets its quarterly workbooks from
a local directory first, then from a real, headed, unmodified Chrome driven by `fetch-uscis.js`
(one browser session per run; the data page is loaded once and every workbook is fetched from
inside it, `delay` ms apart). No fallback to a plain HTTP client exists: if the browser cannot
run, the table fails and the error names the cause.

## Install (once per host)

    govdata/scripts/uscis-browser/install.sh

Pins `puppeteer` 23.11.1 (which downloads Chrome for Testing 131) and prints the path to set
as `USCIS_BROWSER_SCRIPT`. Needs `node` >= 18 and `npm`.

## Host requirements

- A display. Headed Chrome needs `DISPLAY` or `WAYLAND_DISPLAY` in the worker's environment.
  A systemd service does not inherit them: set `Environment=DISPLAY=:0` (WSLg exposes `:0`
  and `wayland-0`) in the unit, or run the worker under a virtual display (`xvfb-run`).
  The helper exits with "no display" when neither is set.
- Shared libraries Chrome links; on Debian/Ubuntu the missing ones are typically
  `libnss3` and `libnspr4` (`apt install libnss3 libnspr4`; a host-package step for the
  maintainer). `ldd <chrome> | grep "not found"` lists what is absent.

## Settings (source parameters of the table, each `${VAR:default}` in `census-schema.yaml`)

| Operand | Env var | Default | Meaning |
|---|---|---|---|
| `uscisLocalDir` | `USCIS_I765_LOCAL_DIR` | unset | directory checked first for `i765_application_for_employment_fy<YYYY>_q<N><suffix>.xlsx` |
| `uscisBrowserScript` | `USCIS_BROWSER_SCRIPT` | unset | path to `fetch-uscis.js` |
| `uscisNodeCommand` | `USCIS_NODE_COMMAND` | `node` | node executable |
| `uscisChromeExecutable` | `USCIS_CHROME_EXECUTABLE` | unset | Chrome to drive (unset: puppeteer's downloaded Chrome) |
| `uscisBrowserCacheDir` | `USCIS_BROWSER_CACHE_DIR` | unset | keeps fetched workbooks and their Last-Modified; later runs send `If-Modified-Since` and a 304 reuses the copy without a download |
| `uscisRequestDelayMs` | `USCIS_REQUEST_DELAY_MS` | 4000 | pause between requests |

Last-Modified is used for the conditional request because it answers 304 to
`If-Modified-Since`; the ETag did not answer 304 to `If-None-Match`.

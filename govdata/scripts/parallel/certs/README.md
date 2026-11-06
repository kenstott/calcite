Extra trust anchors for the worker JVMs (added on top of the JDK's cacerts by common.sh).

- `emsign-ecc-root-ca-g3.pem` — emSign ECC Root CA - G3 (eMudhra), SHA-256
  86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B.
  Fetched from the AIA URL of the emSign Root TLS CA - G3 intermediate. The InCommon-issued
  certificate on www.cbr.washington.edu (fisheries DART) chains to it, and Temurin 21's cacerts
  does not carry it. Chain validated with `openssl verify` before adding.

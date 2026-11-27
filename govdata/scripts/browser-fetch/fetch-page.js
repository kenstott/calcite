#!/usr/bin/env node
// Long-lived helper: one headed Chrome session per ETL run for pages that refuse non-browser
// clients (ssa.gov answers HTTP 403 to plain HTTP, ScienceBase a Cloudflare challenge). Reads one JSON request per stdin line, writes
// one JSON reply per stdout line. All diagnostics go to stderr.
//
//   usage: fetch-page.js <entry-page-url> <delay-ms> [chrome-executable]
//   request: {"url": "<page url on the same origin as the entry page>", "binary": true|false}
//            | {"url": "<file url>", "toFile": "<path>"} to stream the body into that file
//   reply:   {"status": 200, "text": "<response body>"} (or "base64" when binary is true)
//            | {"status": 200, "bytes": <bytes written>} for a toFile request
//            | {"status": 404} | {"error": "<cause>"}
//
// The entry page is loaded once; every URL is then fetched from inside that page (same-origin),
// so the browser's own cookies and headers apply. Plain headed, unmodified Chrome: no stealth
// plugin and no spoofed fingerprint.
'use strict';
const fs = require('fs');
const readline = require('readline');

const [entryPage, delayArg, chromePath] = process.argv.slice(2);
const delayMs = parseInt(delayArg, 10);

function fail(msg) {
  process.stdout.write(JSON.stringify({error: msg}) + '\n');
  process.exit(1);
}

if (!entryPage || !(delayMs >= 0)) {
  fail('usage: fetch-page.js <entry-page-url> <delay-ms> [chrome-executable]');
}
if (!process.env.DISPLAY && !process.env.WAYLAND_DISPLAY) {
  fail('no display: headed Chrome needs DISPLAY or WAYLAND_DISPLAY (WSLg or a virtual display)');
}

let puppeteer;
try {
  puppeteer = require('puppeteer');
} catch (e) {
  fail('puppeteer is not installed (run govdata/scripts/browser-fetch/install.sh): ' + e.message);
}

async function main() {
  // protocolTimeout bounds one in-page fetch, including a large file download.
  const opts = {headless: false, protocolTimeout: 900000};
  if (chromePath && chromePath.length > 0) {
    opts.executablePath = chromePath;
  }
  let browser;
  try {
    browser = await puppeteer.launch(opts);
  } catch (e) {
    fail('cannot start headed Chrome: ' + e.message);
  }
  const page = await browser.newPage();
  // Receives the chunks of a toFile download, one at a time.
  let outFile = null;
  let outBytes = 0;
  await page.exposeFunction('writeChunk', b64 => {
    const chunk = Buffer.from(b64, 'base64');
    fs.writeFileSync(outFile, chunk);
    outBytes += chunk.length;
  });
  const resp = await page.goto(entryPage, {waitUntil: 'domcontentloaded', timeout: 90000});
  if (!resp || resp.status() !== 200) {
    await browser.close();
    fail(entryPage + ' answered HTTP ' + (resp ? resp.status() : 'none') + ' to the headed browser');
  }

  let lastRequest = 0;
  const rl = readline.createInterface({input: process.stdin});
  for await (const line of rl) {
    if (!line.trim()) {
      continue;
    }
    let reply;
    try {
      const request = JSON.parse(line);
      const url = request.url;
      const binary = request.binary === true;
      const wait = lastRequest + delayMs - Date.now();
      if (wait > 0) {
        await new Promise(r => setTimeout(r, wait));
      }
      const toFile = request.toFile !== undefined;
      if (toFile) {
        outFile = fs.openSync(request.toFile, 'w');
        outBytes = 0;
      }
      try {
        reply = await page.evaluate(async (u, bin, stream) => {
          const base64 = bytes => {
            let s = '';
            for (let i = 0; i < bytes.length; i += 8192) {
              s += String.fromCharCode.apply(null, bytes.subarray(i, i + 8192));
            }
            return btoa(s);
          };
          const res = await fetch(u, {credentials: 'same-origin'});
          if (res.status !== 200) {
            return {status: res.status};
          }
          if (stream) {
            const reader = res.body.getReader();
            for (let part = await reader.read(); !part.done; part = await reader.read()) {
              await window.writeChunk(base64(part.value));
            }
            return {status: 200};
          }
          if (!bin) {
            return {status: 200, text: await res.text()};
          }
          return {status: 200, base64: base64(new Uint8Array(await res.arrayBuffer()))};
        }, url, binary, toFile);
      } finally {
        if (toFile) {
          fs.closeSync(outFile);
        }
      }
      if (toFile) {
        reply.bytes = outBytes;
      }
      lastRequest = Date.now();
      if (reply.status !== 200 && reply.status !== 404) {
        reply = {error: 'HTTP ' + reply.status + ' from ' + url};
      }
    } catch (e) {
      reply = {error: e.name + ': ' + e.message + ' (' + line.trim() + ')'};
    }
    process.stdout.write(JSON.stringify(reply) + '\n');
  }
  await browser.close();
}

main().catch(e => fail(e.message));

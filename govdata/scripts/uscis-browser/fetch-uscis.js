#!/usr/bin/env node
// Long-lived helper: one headed Chrome session per ETL run. Reads one JSON request per stdin
// line, writes one JSON reply per stdout line. All diagnostics go to stderr.
//
//   usage: fetch-uscis.js <data-page-url> <delay-ms> [cache-dir] [chrome-executable]
//   request: {"url": "<workbook url>"}
//   reply:   {"status": 200|304|404, "file": "<path>"}  or  {"error": "<cause>"}
//
// The data page is loaded once; every workbook is then fetched from inside that page
// (same-origin), so the browser's own cookies and headers apply. With a cache dir, a workbook
// seen before is requested with If-Modified-Since and a 304 reuses the cached copy.
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const readline = require('readline');

const [dataPage, delayArg, cacheDirArg, chromePath] = process.argv.slice(2);
const delayMs = parseInt(delayArg, 10);

function fail(msg) {
  process.stdout.write(JSON.stringify({error: msg}) + '\n');
  process.exit(1);
}

if (!dataPage || !(delayMs >= 0)) {
  fail('usage: fetch-uscis.js <data-page-url> <delay-ms> [cache-dir] [chrome-executable]');
}
if (!process.env.DISPLAY && !process.env.WAYLAND_DISPLAY) {
  fail('no display: headed Chrome needs DISPLAY or WAYLAND_DISPLAY (WSLg or a virtual display)');
}

let puppeteer;
try {
  puppeteer = require('puppeteer');
} catch (e) {
  fail('puppeteer is not installed (run govdata/scripts/uscis-browser/install.sh): ' + e.message);
}

const cacheDir = cacheDirArg && cacheDirArg.length > 0 ? cacheDirArg : null;
if (cacheDir) {
  fs.mkdirSync(cacheDir, {recursive: true});
}
const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 'uscis-wb-'));

function cacheKey(url) {
  return crypto.createHash('sha256').update(url).digest('hex');
}

async function main() {
  const opts = {headless: false};
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
  const resp = await page.goto(dataPage, {waitUntil: 'domcontentloaded', timeout: 90000});
  if (!resp || resp.status() !== 200) {
    await browser.close();
    fail('uscis.gov data page answered HTTP ' + (resp ? resp.status() : 'none') + ' to the headed browser');
  }

  let lastRequest = 0;
  const rl = readline.createInterface({input: process.stdin});
  for await (const line of rl) {
    if (!line.trim()) {
      continue;
    }
    let reply;
    try {
      const url = JSON.parse(line).url;
      const wait = lastRequest + delayMs - Date.now();
      if (wait > 0) {
        await new Promise(r => setTimeout(r, wait));
      }
      reply = await fetchOne(page, url);
      lastRequest = Date.now();
    } catch (e) {
      reply = {error: e.message};
    }
    process.stdout.write(JSON.stringify(reply) + '\n');
  }
  await browser.close();
  fs.rmSync(outDir, {recursive: true, force: true});
}

async function fetchOne(page, url) {
  const key = cacheKey(url);
  const cached = cacheDir ? path.join(cacheDir, key + '.xlsx') : null;
  const meta = cacheDir ? path.join(cacheDir, key + '.lm') : null;
  const haveCache = cached && fs.existsSync(cached) && fs.existsSync(meta);
  const ims = haveCache ? fs.readFileSync(meta, 'utf8') : null;

  const r = await page.evaluate(async (u, since) => {
    const headers = since ? {'If-Modified-Since': since} : {};
    const res = await fetch(u, {credentials: 'same-origin', headers});
    if (res.status !== 200) {
      return {status: res.status};
    }
    const buf = new Uint8Array(await res.arrayBuffer());
    let bin = '';
    for (let i = 0; i < buf.length; i += 0x8000) {
      bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
    }
    return {status: 200, lastModified: res.headers.get('last-modified'), body: btoa(bin)};
  }, url, ims);

  if (r.status === 304 && haveCache) {
    return {status: 304, file: cached};
  }
  if (r.status === 404) {
    return {status: 404};
  }
  if (r.status !== 200) {
    return {error: 'HTTP ' + r.status + ' from ' + url};
  }
  const target = cached || path.join(outDir, key + '.xlsx');
  fs.writeFileSync(target, Buffer.from(r.body, 'base64'));
  if (meta && r.lastModified) {
    fs.writeFileSync(meta, r.lastModified);
  }
  return {status: 200, file: target};
}

main().catch(e => fail(e.message));

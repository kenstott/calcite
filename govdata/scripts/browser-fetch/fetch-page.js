#!/usr/bin/env node
// Long-lived helper: one headed Chrome session per ETL run for pages that refuse non-browser
// clients (ssa.gov answers HTTP 403 to plain HTTP). Reads one JSON request per stdin line, writes
// one JSON reply per stdout line. All diagnostics go to stderr.
//
//   usage: fetch-page.js <entry-page-url> <delay-ms> [chrome-executable]
//   request: {"url": "<page url on the same origin as the entry page>"}
//   reply:   {"status": 200, "text": "<response body>"} | {"status": 404} | {"error": "<cause>"}
//
// The entry page is loaded once; every URL is then fetched from inside that page (same-origin),
// so the browser's own cookies and headers apply. Plain headed, unmodified Chrome: no stealth
// plugin and no spoofed fingerprint.
'use strict';
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
      const url = JSON.parse(line).url;
      const wait = lastRequest + delayMs - Date.now();
      if (wait > 0) {
        await new Promise(r => setTimeout(r, wait));
      }
      reply = await page.evaluate(async (u) => {
        const res = await fetch(u, {credentials: 'same-origin'});
        if (res.status !== 200) {
          return {status: res.status};
        }
        return {status: 200, text: await res.text()};
      }, url);
      lastRequest = Date.now();
      if (reply.status !== 200 && reply.status !== 404) {
        reply = {error: 'HTTP ' + reply.status + ' from ' + url};
      }
    } catch (e) {
      reply = {error: e.message};
    }
    process.stdout.write(JSON.stringify(reply) + '\n');
  }
  await browser.close();
}

main().catch(e => fail(e.message));

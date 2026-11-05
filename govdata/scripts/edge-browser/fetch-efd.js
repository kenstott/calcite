#!/usr/bin/env node
// Long-lived helper: one Windows-Edge session per ETL run for efdsearch.senate.gov. Reads one JSON
// request per stdin line, writes one JSON reply per stdout line. Diagnostics go to stderr.
//
//   usage: fetch-efd.js <edge-exe> <debug-port> <profile-dir-windows-path>
//   {"op":"agree"}                              -> {"ok":true}
//   {"op":"get","path":"/search/view/..."}      -> {"status":N,"finalPath":"/...","body":"..."}
//   {"op":"post","path":"/...","fields":{...}}  -> {"status":N,"body":"..."}   (X-CSRFToken from the cookie)
//   or {"error":"<cause>"}
//
// The agreement is accepted by clicking through the page like a person; later requests are issued
// from inside that page (same origin), so the browser's own cookies and headers apply.
'use strict';
const readline = require('readline');

const [edgeExe, portArg, profileBase] = process.argv.slice(2);
const SITE = 'https://efdsearch.senate.gov';

function fail(msg) {
  process.stdout.write(JSON.stringify({error: msg}) + '\n');
  process.exit(1);
}
if (!edgeExe || !(parseInt(portArg, 10) > 0) || !profileBase) {
  fail('usage: fetch-efd.js <edge-exe> <debug-port> <profile-dir>');
}

let session;
try {
  session = require('./edge-session');
} catch (e) {
  fail(e.message);
}

async function agree(page) {
  const resp = await page.goto(SITE + '/search/home/', {waitUntil: 'domcontentloaded', timeout: 90000});
  if (!resp || resp.status() !== 200) {
    throw new Error('eFD home answered HTTP ' + (resp ? resp.status() : 'none') + ' to Edge');
  }
  const box = await page.$('#agree_statement');
  if (!box) {
    throw new Error('eFD home page has no agreement checkbox (#agree_statement)');
  }
  // ticking the checkbox submits the agreement form; the session is open once the browser lands on /search/
  await box.click();
  try {
    await page.waitForFunction(() => location.pathname === '/search/', {timeout: 60000});
  } catch (e) {
    throw new Error('eFD agreement did not reach /search/ (stayed at ' + page.url() + '): ' + e.message);
  }
}

async function request(page, req) {
  return page.evaluate(async (site, r) => {
    const init = {credentials: 'same-origin', redirect: 'follow'};
    if (r.op === 'post') {
      const csrf = (document.cookie.split('; ').find(c => c.startsWith('csrftoken=')) || '').slice(10);
      if (!csrf) {
        return {error: 'the session has no csrftoken cookie'};
      }
      init.method = 'POST';
      init.headers = {'Content-Type': 'application/x-www-form-urlencoded', 'X-CSRFToken': csrf};
      init.body = new URLSearchParams(r.fields).toString();
    }
    const res = await fetch(site + r.path, init);
    return {status: res.status, finalPath: new URL(res.url).pathname, redirected: res.redirected,
        body: await res.text()};
  }, SITE, req);
}

async function main() {
  let edge;
  try {
    edge = await session.start(edgeExe, parseInt(portArg, 10), profileBase);
  } catch (e) {
    fail(e.message);
  }
  const page = await edge.browser.newPage();
  let agreed = false;
  const rl = readline.createInterface({input: process.stdin});
  for await (const line of rl) {
    if (!line.trim()) {
      continue;
    }
    let reply;
    try {
      const req = JSON.parse(line);
      if (req.op === 'agree') {
        if (!agreed) {
          await agree(page);
          agreed = true;
        }
        reply = {ok: true};
      } else if (req.op === 'get' || req.op === 'post') {
        if (!agreed) {
          throw new Error('request before the agreement was accepted');
        }
        reply = await request(page, req);
      } else {
        reply = {error: 'unknown op ' + req.op};
      }
    } catch (e) {
      reply = {error: e.message};
    }
    process.stdout.write(JSON.stringify(reply) + '\n');
  }
  await edge.close();
}

main().catch(e => fail(e.message));

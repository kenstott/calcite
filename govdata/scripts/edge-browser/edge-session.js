'use strict';
// Starts a Windows-side Edge from WSL with a fresh temporary profile and a DevTools port bound to
// localhost, and connects Puppeteer to it. The browser is headed and unmodified. close() shuts the
// browser down through the protocol, waits for the process to exit and deletes the profile, so no
// debug port stays open.
const {spawn, execFileSync} = require('child_process');
const fs = require('fs');
const http = require('http');

let puppeteer;
try {
  puppeteer = require('puppeteer-core');
} catch (e) {
  throw new Error('puppeteer-core is not installed (run govdata/scripts/edge-browser/install.sh): ' + e.message);
}

function portReachable(port) {
  return new Promise(resolve => {
    const req = http.get({host: '127.0.0.1', port, path: '/json/version', timeout: 1000}, res => {
      res.resume();
      resolve(res.statusCode === 200);
    });
    req.on('error', () => resolve(false));
    req.on('timeout', () => { req.destroy(); resolve(false); });
  });
}

async function start(edgeExe, port, profileBase) {
  if (!fs.existsSync(edgeExe)) {
    throw new Error('Edge not found at ' + edgeExe + ' (set efdEdgeExecutable / EFD_EDGE_EXECUTABLE)');
  }
  if (await portReachable(port)) {
    throw new Error('debug port ' + port + ' is already in use; refusing to drive a browser this run did not start');
  }
  const profile = profileBase.replace(/[\\/]+$/, '') + '\\efd-' + process.pid + '-' + Date.now();
  const proc = spawn(edgeExe, ['--remote-debugging-port=' + port, '--user-data-dir=' + profile,
    '--no-first-run', '--no-default-browser-check', 'about:blank'], {stdio: 'ignore'});
  let spawnError = null;
  proc.on('error', e => { spawnError = e; });
  const deadline = Date.now() + 30000;
  while (!(await portReachable(port))) {
    if (spawnError) {
      throw new Error('cannot start Edge ' + edgeExe + ': ' + spawnError.message);
    }
    if (Date.now() > deadline) {
      throw new Error('Edge debug port ' + port + ' not reachable after 30s');
    }
    await new Promise(r => setTimeout(r, 300));
  }
  const browser = await puppeteer.connect({browserURL: 'http://127.0.0.1:' + port, defaultViewport: null});
  return {
    browser,
    async close() {
      try {
        await browser.close();
      } finally {
        const gone = Date.now() + 15000;
        while (Date.now() < gone && await portReachable(port)) {
          await new Promise(r => setTimeout(r, 300));
        }
        if (await portReachable(port)) {
          throw new Error('Edge debug port ' + port + ' still open after browser.close()');
        }
        // Edge releases the profile files a moment after its port closes
        await new Promise(r => setTimeout(r, 1500));
        const wsl = execFileSync('wslpath', ['-u', profile]).toString().trim();
        fs.rmSync(wsl, {recursive: true, force: true});
      }
    }
  };
}

module.exports = {start};

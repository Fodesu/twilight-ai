// Live Grafana frontend acceptance using Chrome's CDP and Node built-ins only.
// Uses a temporary profile, anonymous access, and cleans up its own browser.
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

process.umask(0o077);
const roles = ['owner', 'worker', 'model-backend', 'tool-backend'];
const labels = ['Owner（会话与命令）', 'Worker（执行与恢复）', 'Model Backend（模型执行）', 'Tool Backend（工具与工作区）'];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const base = process.env.GRAFANA_URL || 'http://127.0.0.1:13000';
const binary = process.env.CHROME_BIN || (process.platform === 'darwin' ? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' : 'chromium');
const evidence = process.env.VALIDATION_BROWSER_OUTPUT || await mkdtemp(join(tmpdir(), 'twilight-dashboard-evidence-'));
if (process.env.VALIDATION_BROWSER_OUTPUT) await mkdir(evidence, { mode: 0o700 });
console.log('Browser evidence:', evidence);
let port;
const profile = await mkdtemp(join(tmpdir(), 'twilight-grafana-browser-'));
const browser = spawn(binary, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
  '--remote-debugging-address=127.0.0.1', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore' });
let ws;
let nextId = 0;
const pending = new Map();
let expressions = [];

function command(method, params = {}) {
  const id = ++nextId;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)); }, 20000);
    pending.set(id, { resolve, reject, timer });
    ws.send(JSON.stringify({ id, method, params }));
  });
}
async function evaluate(expression) {
  const r = await command('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  assert(!r.exceptionDetails, JSON.stringify(r.exceptionDetails));
  return r.result.value;
}
function matcher(expr, key) {
  const token = expr.match(new RegExp(`${key}=~("(?:\\\\.|[^"\\\\])*")`));
  return token ? JSON.parse(token[1]) : null;
}
function accepts(pattern, value) { return new RegExp(`^(?:${pattern})$`).test(value); }

try {
  let tabs;
  for (let i = 0; i < 50; i++) {
    try {
      port = Number((await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]);
      tabs = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
      if (tabs.some(t => t.type === 'page')) break;
    } catch {}
    await sleep(200);
  }
  assert(tabs?.some(t => t.type === 'page'), 'Chrome did not start');
  ws = new WebSocket(tabs.find(t => t.type === 'page').webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, { once: true }); ws.addEventListener('error', reject, { once: true }); });
  ws.addEventListener('message', event => {
    const msg = JSON.parse(event.data);
    if (pending.has(msg.id)) {
      const p = pending.get(msg.id); pending.delete(msg.id); clearTimeout(p.timer);
      if (msg.error) p.reject(new Error(JSON.stringify(msg.error))); else p.resolve(msg.result);
    }
    if (msg.method === 'Network.requestWillBeSent') {
      const request = msg.params.request;
      if (request.url.includes('/api/ds/query') && request.postData) {
        try { expressions.push(...JSON.parse(request.postData).queries.map(q => q.expr).filter(Boolean)); } catch {}
      }
    }
  });
  await command('Page.enable');
  await command('Network.enable', { maxPostDataSize: 1024 * 1024 });
  await command('Runtime.enable');
  await command('Emulation.setDeviceMetricsOverride', { width: 1600, height: 1100, deviceScaleFactor: 1, mobile: false });
  await command('Page.navigate', { url: base + '/d/twilight-validation' });
  for (let i = 0; i < 60; i++) {
    if ((await evaluate('document.body?.innerText || ""')).includes('普通 RPC：当前活跃请求')) break;
    await sleep(500);
  }
  let text = '';
  // Scroll the real dashboard container: Grafana lazily runs off-screen panels.
  for (let step = 0; step < 16; step++) {
    text += '\n' + await evaluate('document.body?.innerText || ""');
    const hasMore = await evaluate(`(() => {
      const root = document.scrollingElement;
      const e = root.scrollHeight > root.clientHeight + 100 ? root : ([...document.querySelectorAll('*')].filter(e => e.scrollHeight > e.clientHeight + 100 && /auto|scroll/.test(getComputedStyle(e).overflowY)).sort((a,b) => (b.scrollHeight-b.clientHeight)-(a.scrollHeight-a.clientHeight))[0] || root);
      if (e.scrollTop + e.clientHeight >= e.scrollHeight - 5) return false;
      e.scrollTop += Math.max(e.clientHeight - 100, 600); return true;
    })()`);
    await sleep(1200);
    if (!hasMore) break;
  }
  for (const label of labels) assert(text.includes(label + ' · 组件详情'), `Missing rendered component row: ${label}`);
  const classQueries = expressions.filter(e => e.includes('twilight_http_requests_in_flight_by_class'));
  const observed = new Set();
  for (const expr of classQueries) {
    const role = matcher(expr, 'role');
    const matches = roles.filter(r => accepts(role, r));
    assert.equal(matches.length, 1, `Repeated row query mixes components: ${expr}`);
    observed.add(matches[0]);
  }
  assert.equal(observed.size, 4, 'Not all four repeated rows ran scoped queries');
  console.log('Rendered four separate component rows; frontend requests are component-scoped:', [...observed]);
  await evaluate('document.scrollingElement.scrollTop = 0');
  await sleep(500);
  const overviewImage = await command('Page.captureScreenshot', { format: 'png' });
  const overviewOutput = join(evidence, 'overview.png');
  await writeFile(overviewOutput, Buffer.from(overviewImage.data, 'base64'));
  console.log('Overview screenshot:', overviewOutput);

  const pod = execFileSync('kubectl', ['--context', 'k3d-twilight', '-n', 'twilight-validation', 'get', 'pods', '-l', 'app.kubernetes.io/name=worker', '-o', 'jsonpath={.items[0].metadata.name}'], { encoding: 'utf8' });
  await command('Page.navigate', { url: base + '/d/twilight-validation?var-component=worker&var-pod=' + encodeURIComponent(pod) });
  for (let i = 0; i < 60; i++) {
    const body = await evaluate('document.body?.innerText || ""');
    if (body.includes('Worker（执行与恢复） · 组件详情') && !body.includes('Owner（会话与命令） · 组件详情')) break;
    await sleep(500);
  }
  expressions = [];
  // Bring the class panels into view before testing the lazy frontend queries.
  await evaluate('document.scrollingElement.scrollTop = document.scrollingElement.clientHeight');
  // Wait for a fresh refresh to avoid counting queries from the previous page.
  await evaluate(`(() => { const b = [...document.querySelectorAll('button')].find(b => /refresh dashboard/i.test((b.getAttribute('aria-label') || '') + ' ' + b.title) || /RefreshPicker run button/.test(b.getAttribute('data-testid') || '')); if (b) b.click(); })()`);
  for (let i = 0; i < 40 && !expressions.some(e => e.includes('twilight_http_requests_in_flight_by_class')); i++) await sleep(500);
  const selected = expressions.filter(e => e.includes('twilight_http_requests_in_flight_by_class'));
  assert(selected.length, 'No frontend class query for selected worker/POD');
  for (const expr of selected) {
    assert.deepEqual(roles.filter(r => accepts(matcher(expr, 'role'), r)), ['worker'], expr);
    assert(accepts(matcher(expr, 'pod'), pod), expr);
    assert(!accepts(matcher(expr, 'pod'), 'other-pod'), `POD selector not scoped: ${expr}`);
  }
  console.log('Single component + single POD frontend filtering verified:', pod);
  const screenshot = await command('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false });
  const output = join(evidence, 'selected-pod.png');
  await writeFile(output, Buffer.from(screenshot.data, 'base64'));
  console.log('Screenshot:', output);
} catch (error) {
  if (ws?.readyState === WebSocket.OPEN) {
    try {
      await writeFile(join(evidence, 'failure.json'), JSON.stringify({
        body: await evaluate('document.body?.innerText || ""'), expressions,
        scrollables: await evaluate(`([...document.querySelectorAll('*')].filter(e => e.scrollHeight > e.clientHeight + 100).map(e => ({tag:e.tagName,cls:e.className,testid:e.getAttribute('data-testid'),height:e.clientHeight,total:e.scrollHeight,top:e.scrollTop,overflow:getComputedStyle(e).overflowY})))`),
      }, null, 2));
      const image = await command('Page.captureScreenshot', { format: 'png' });
      await writeFile(join(evidence, 'failure.png'), Buffer.from(image.data, 'base64'));
    } catch {}
  }
  throw error;
} finally {
  if (ws) ws.close();
  browser.kill('SIGTERM');
  await sleep(1000);
  if (browser.exitCode === null) browser.kill('SIGKILL');
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
}

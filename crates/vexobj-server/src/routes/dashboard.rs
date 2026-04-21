use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;

use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new().route("/dashboard", get(dashboard))
}

async fn dashboard() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VexObj Dashboard</title>
<style>
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --border: #2a2d3a;
    --text: #e4e4e7;
    --muted: #71717a;
    --accent: #6366f1;
    --accent-hover: #818cf8;
    --green: #22c55e;
    --red: #ef4444;
    --yellow: #eab308;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
  }
  .header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 16px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .header h1 { font-size: 20px; font-weight: 600; letter-spacing: -0.01em; }
  .header h1 span { color: var(--accent); }
  .header .version { color: var(--muted); font-size: 13px; }
  .auth-bar {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 12px 24px;
    display: flex;
    gap: 12px;
    align-items: center;
  }
  .auth-bar input {
    flex: 1;
    padding: 8px 12px;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 14px;
    font-family: monospace;
  }
  .auth-bar input:focus { outline: none; border-color: var(--accent); }
  button {
    padding: 8px 16px;
    background: var(--accent);
    color: white;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-size: 14px;
    font-weight: 500;
  }
  button:hover { background: var(--accent-hover); }
  button.danger { background: var(--red); }
  button.danger:hover { background: #dc2626; }
  button.secondary { background: var(--border); }
  button.secondary:hover { background: #3a3d4a; }
  button.small { padding: 4px 10px; font-size: 12px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
  }
  .card .label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
  .card .value { font-size: 28px; font-weight: 700; }
  .card .sub { font-size: 13px; color: var(--muted); margin-top: 4px; }
  .section { margin-bottom: 24px; }
  .section-title { font-size: 16px; font-weight: 600; margin-bottom: 12px; display: flex; align-items: center; justify-content: space-between; }
  .breadcrumb { font-size: 14px; color: var(--muted); margin-bottom: 16px; }
  .breadcrumb a { color: var(--accent); text-decoration: none; cursor: pointer; }
  .breadcrumb a:hover { text-decoration: underline; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; padding: 8px 12px; border-bottom: 1px solid var(--border); }
  td { padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 14px; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover td { background: rgba(99,102,241,0.05); }
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
  }
  .badge.green { background: rgba(34,197,94,0.15); color: var(--green); }
  .badge.red { background: rgba(239,68,68,0.15); color: var(--red); }
  .badge.yellow { background: rgba(234,179,8,0.15); color: var(--yellow); }
  .badge.muted { background: rgba(113,113,122,0.15); color: var(--muted); }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
  .status { display: flex; align-items: center; gap: 8px; }
  .dot { width: 8px; height: 8px; border-radius: 50%; }
  .dot.green { background: var(--green); }
  .empty { color: var(--muted); text-align: center; padding: 40px; }
  .modal-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.6);
    z-index: 100;
    align-items: center;
    justify-content: center;
  }
  .modal-overlay.active { display: flex; }
  .modal {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px;
    min-width: 400px;
    max-width: 500px;
  }
  .modal h3 { margin-bottom: 16px; }
  .form-group { margin-bottom: 12px; }
  .form-group label { display: block; font-size: 13px; color: var(--muted); margin-bottom: 4px; }
  .form-group input, .form-group select {
    width: 100%;
    padding: 8px 12px;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 14px;
  }
  .form-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px; }
  .alert {
    padding: 12px 16px;
    border-radius: 6px;
    margin-bottom: 16px;
    font-size: 14px;
    font-family: monospace;
    word-break: break-all;
    background: rgba(99,102,241,0.1);
    border: 1px solid var(--accent);
  }
  .alert.err {
    background: rgba(239,68,68,0.1);
    border-color: var(--red);
    color: var(--red);
  }
  .hidden { display: none; }
  .flex { display: flex; gap: 8px; }
  .drop-zone {
    border: 2px dashed var(--border);
    border-radius: 8px;
    padding: 32px;
    text-align: center;
    color: var(--muted);
    transition: all 0.15s;
    cursor: pointer;
  }
  .drop-zone:hover, .drop-zone.drag-over {
    border-color: var(--accent);
    color: var(--text);
    background: rgba(99,102,241,0.05);
  }
  .drop-zone input { display: none; }
  .upload-status {
    margin-top: 8px;
    font-size: 13px;
    font-family: monospace;
  }
  .upload-row {
    display: flex;
    justify-content: space-between;
    padding: 4px 0;
    color: var(--muted);
  }
  .upload-row.done { color: var(--green); }
  .upload-row.err { color: var(--red); }
</style>
</head>
<body>
<div class="header">
  <h1><span>vex</span>obj</h1>
  <div class="status">
    <div class="dot green"></div>
    <span class="version" id="version">v0.1.0</span>
  </div>
</div>
<div class="auth-bar">
  <input type="password" id="apiKey" placeholder="Enter your API key (vex_...)" />
  <button onclick="connect()">Connect</button>
</div>
<div class="container" id="app">
  <div class="empty">Enter your API key to access the dashboard</div>
</div>

<!-- Create Bucket Modal -->
<div class="modal-overlay" id="createBucketModal">
  <div class="modal">
    <h3>Create Bucket</h3>
    <div class="form-group">
      <label>Bucket Name</label>
      <input id="bucketName" placeholder="my-bucket" />
    </div>
    <div class="form-group">
      <label><input type="checkbox" id="bucketPublic"> Public (unauthenticated reads allowed)</label>
    </div>
    <div class="form-actions">
      <button class="secondary" onclick="closeModal('createBucketModal')">Cancel</button>
      <button onclick="createBucket()">Create</button>
    </div>
  </div>
</div>

<!-- Create Key Modal -->
<div class="modal-overlay" id="createKeyModal">
  <div class="modal">
    <h3>Create API Key</h3>
    <div class="form-group">
      <label>Key Name</label>
      <input id="keyName" placeholder="my-app" />
    </div>
    <div class="form-group">
      <label>Permissions</label>
      <div class="flex" style="flex-wrap:wrap;gap:12px;margin-top:4px;">
        <label><input type="checkbox" id="permRead" checked> Read</label>
        <label><input type="checkbox" id="permWrite"> Write</label>
        <label><input type="checkbox" id="permDelete"> Delete</label>
        <label><input type="checkbox" id="permAdmin"> Admin</label>
      </div>
    </div>
    <div class="form-actions">
      <button class="secondary" onclick="closeModal('createKeyModal')">Cancel</button>
      <button onclick="createKey()">Create</button>
    </div>
  </div>
</div>

<script>
const BASE = window.location.origin;
let KEY = '';
// Simple in-memory state. No framework — re-render on every action.
let STATE = { stats: null, buckets: [], keys: [], currentBucket: null, objects: null };

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
async function api(path, opts = {}) {
  const resp = await fetch(BASE + path, {
    ...opts,
    headers: { 'Authorization': 'Bearer ' + KEY, 'Content-Type': 'application/json', ...(opts.headers || {}) },
  });
  if (!resp.ok) {
    const body = await resp.text();
    let err;
    try { err = JSON.parse(body); } catch (_) { err = { error: body || resp.statusText }; }
    err.__status = resp.status;
    throw err;
  }
  const ct = resp.headers.get('content-type') || '';
  return ct.includes('application/json') ? resp.json() : resp.blob();
}

function humanSize(bytes) {
  if (bytes == null) return '';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0, n = bytes;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return n.toFixed(n < 10 && i > 0 ? 1 : 0) + ' ' + units[i];
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function showErr(msg) {
  const app = document.getElementById('app');
  const existing = app.querySelector('.alert.err');
  if (existing) existing.remove();
  const el = document.createElement('div');
  el.className = 'alert err';
  el.textContent = msg;
  app.prepend(el);
  setTimeout(() => el.remove(), 5000);
}

// ---------------------------------------------------------------------------
// Connect & routing
// ---------------------------------------------------------------------------
async function connect() {
  KEY = document.getElementById('apiKey').value.trim();
  if (!KEY) return;
  try {
    STATE.stats = await api('/v1/stats');
    document.getElementById('version').textContent = 'v' + STATE.stats.version;
    await route();
  } catch (e) {
    document.getElementById('app').innerHTML =
      '<div class="empty">Invalid API key or insufficient permissions (admin required for stats)</div>';
  }
}

async function route() {
  const hash = window.location.hash;
  const bucketMatch = hash.match(/^#bucket=([^&]+)/);
  if (bucketMatch) {
    STATE.currentBucket = decodeURIComponent(bucketMatch[1]);
    await renderBucket();
  } else {
    STATE.currentBucket = null;
    await renderDashboard();
  }
}

window.addEventListener('hashchange', () => { if (KEY) route(); });

// ---------------------------------------------------------------------------
// Dashboard view
// ---------------------------------------------------------------------------
async function renderDashboard() {
  const [buckets, keys] = await Promise.all([
    api('/v1/buckets').then(r => r.buckets || []).catch(() => []),
    api('/v1/admin/keys').then(r => r.keys || []).catch(() => []),
  ]);
  STATE.buckets = buckets;
  STATE.keys = keys;

  const s = STATE.stats || {};
  const details = s.bucket_details || [];
  const app = document.getElementById('app');
  app.innerHTML = `
    <div class="grid">
      <div class="card">
        <div class="label">Buckets</div>
        <div class="value">${s.buckets ?? buckets.length}</div>
      </div>
      <div class="card">
        <div class="label">Objects</div>
        <div class="value">${s.total_objects ?? '—'}</div>
      </div>
      <div class="card">
        <div class="label">Storage</div>
        <div class="value">${s.total_size_human ?? '—'}</div>
        <div class="sub">Disk: ${s.disk_usage_human ?? '—'}</div>
      </div>
      <div class="card">
        <div class="label">API Keys</div>
        <div class="value">${keys.length}</div>
      </div>
    </div>

    <div class="section">
      <div class="section-title">
        <span>Buckets</span>
        <button onclick="openModal('createBucketModal')">+ New Bucket</button>
      </div>
      <div class="card">
        <table>
          <thead><tr><th>Name</th><th>Visibility</th><th>Objects</th><th>Size</th><th>Created</th><th></th></tr></thead>
          <tbody id="bucketsTable"></tbody>
        </table>
      </div>
    </div>

    <div class="section">
      <div class="section-title">
        <span>API Keys</span>
        <button onclick="openModal('createKeyModal')">+ New Key</button>
      </div>
      <div id="keyAlert" class="alert hidden"></div>
      <div class="card">
        <table>
          <thead><tr><th>Name</th><th>Prefix</th><th>Permissions</th><th>Created</th><th></th></tr></thead>
          <tbody id="keysTable"></tbody>
        </table>
      </div>
    </div>
  `;

  const bucketsTable = document.getElementById('bucketsTable');
  for (const b of buckets) {
    const d = details.find(x => x.name === b.name) || { objects: 0, size_human: '0 B' };
    const row = document.createElement('tr');
    row.className = 'clickable';
    row.innerHTML = `
      <td><strong>${esc(b.name)}</strong></td>
      <td>${b.public ? '<span class="badge yellow">public</span>' : '<span class="badge muted">private</span>'}</td>
      <td>${d.objects}</td>
      <td>${d.size_human}</td>
      <td class="mono">${new Date(b.created_at).toLocaleDateString()}</td>
      <td><button class="danger small" data-act="del">Delete</button></td>
    `;
    row.addEventListener('click', (e) => {
      if (e.target.dataset.act === 'del') {
        e.stopPropagation();
        deleteBucket(b.name);
      } else {
        window.location.hash = 'bucket=' + encodeURIComponent(b.name);
      }
    });
    bucketsTable.appendChild(row);
  }
  if (!buckets.length) bucketsTable.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--muted)">No buckets yet</td></tr>';

  const keysTable = document.getElementById('keysTable');
  for (const k of keys) {
    const perms = [];
    if (k.permissions.read) perms.push('<span class="badge green">read</span>');
    if (k.permissions.write) perms.push('<span class="badge yellow">write</span>');
    if (k.permissions.delete) perms.push('<span class="badge red">delete</span>');
    if (k.permissions.admin) perms.push('<span class="badge" style="background:rgba(99,102,241,0.15);color:var(--accent)">admin</span>');
    keysTable.innerHTML += `<tr>
      <td><strong>${esc(k.name)}</strong></td>
      <td class="mono">${esc(k.key_prefix)}...</td>
      <td>${perms.join(' ')}</td>
      <td class="mono">${new Date(k.created_at).toLocaleDateString()}</td>
      <td><button class="danger small" onclick="deleteKey('${esc(k.id)}')">Revoke</button></td>
    </tr>`;
  }
  if (!keys.length) keysTable.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted)">No API keys yet</td></tr>';
}

// ---------------------------------------------------------------------------
// Bucket detail view (object browser + upload)
// ---------------------------------------------------------------------------
async function renderBucket() {
  const bucket = STATE.currentBucket;
  const app = document.getElementById('app');
  app.innerHTML = `
    <div class="breadcrumb"><a onclick="goHome()">← Buckets</a> / <strong>${esc(bucket)}</strong></div>

    <div class="section">
      <div class="section-title">
        <span>Upload</span>
      </div>
      <label class="drop-zone" id="dropZone">
        <input type="file" id="fileInput" multiple>
        <div>Drop files here or click to choose</div>
        <div class="upload-status" id="uploadStatus"></div>
      </label>
    </div>

    <div class="section">
      <div class="section-title">
        <span>Objects</span>
        <span class="mono" style="color:var(--muted);font-size:13px" id="objectCount"></span>
      </div>
      <div class="card">
        <table>
          <thead><tr><th>Key</th><th>Size</th><th>Content-Type</th><th>Updated</th><th></th></tr></thead>
          <tbody id="objectsTable"><tr><td colspan="5" class="empty">Loading…</td></tr></tbody>
        </table>
      </div>
    </div>
  `;

  wireUpload(bucket);
  await loadObjects(bucket);
}

async function loadObjects(bucket) {
  const table = document.getElementById('objectsTable');
  try {
    const resp = await api('/v1/objects/' + encodeURIComponent(bucket));
    const objs = resp.objects || [];
    STATE.objects = objs;
    document.getElementById('objectCount').textContent =
      objs.length + ' object' + (objs.length === 1 ? '' : 's');
    if (!objs.length) {
      table.innerHTML = '<tr><td colspan="5" class="empty">No objects yet — upload one above</td></tr>';
      return;
    }
    table.innerHTML = '';
    for (const o of objs) {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td class="mono">${esc(o.key)}</td>
        <td>${humanSize(o.size)}</td>
        <td class="mono" style="color:var(--muted)">${esc(o.content_type || '')}</td>
        <td class="mono">${new Date(o.updated_at).toLocaleString()}</td>
        <td>
          <div class="flex">
            <button class="small secondary" data-act="get">Download</button>
            <button class="small danger" data-act="del">Delete</button>
          </div>
        </td>
      `;
      row.querySelector('[data-act=get]').addEventListener('click', () => downloadObject(bucket, o.key));
      row.querySelector('[data-act=del]').addEventListener('click', () => deleteObject(bucket, o.key));
      table.appendChild(row);
    }
  } catch (e) {
    table.innerHTML = `<tr><td colspan="5" class="empty" style="color:var(--red)">${esc(e.error || 'Failed to list')}</td></tr>`;
  }
}

function wireUpload(bucket) {
  const zone = document.getElementById('dropZone');
  const input = document.getElementById('fileInput');
  input.addEventListener('change', () => uploadFiles(bucket, [...input.files]));
  zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('drag-over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
  zone.addEventListener('drop', (e) => {
    e.preventDefault();
    zone.classList.remove('drag-over');
    uploadFiles(bucket, [...e.dataTransfer.files]);
  });
}

function encodeKey(k) {
  // Preserve `/` path separators — route `/v1/objects/{bucket}/{*key}`
  // captures slashes into the key, but other special chars (#, ?, %, …)
  // must be escaped.
  return k.split('/').map(encodeURIComponent).join('/');
}

async function uploadFiles(bucket, files) {
  if (!files.length) return;
  const status = document.getElementById('uploadStatus');
  status.innerHTML = '';
  for (const f of files) {
    const row = document.createElement('div');
    row.className = 'upload-row';
    row.innerHTML = `<span>${esc(f.name)}</span><span>uploading…</span>`;
    status.appendChild(row);
    try {
      // Native streaming PUT — handles arbitrarily large files without
      // buffering in server RAM, and goes through the engine-level quota
      // check.
      const resp = await fetch(
        BASE + '/v1/objects/' + encodeURIComponent(bucket) + '/' + encodeKey(f.name),
        {
          method: 'PUT',
          headers: {
            'Authorization': 'Bearer ' + KEY,
            'Content-Type': f.type || 'application/octet-stream',
          },
          body: f,
        }
      );
      if (!resp.ok) {
        let body = await resp.text();
        try { body = JSON.parse(body).error || body; } catch (_) {}
        row.className = 'upload-row err';
        row.innerHTML = `<span>${esc(f.name)}</span><span>HTTP ${resp.status}: ${esc(body).slice(0, 80)}</span>`;
      } else {
        row.className = 'upload-row done';
        row.innerHTML = `<span>${esc(f.name)}</span><span>✓ ${humanSize(f.size)}</span>`;
      }
    } catch (e) {
      row.className = 'upload-row err';
      row.innerHTML = `<span>${esc(f.name)}</span><span>${esc(e.message || 'error')}</span>`;
    }
  }
  await loadObjects(bucket);
}

async function downloadObject(bucket, key) {
  // Fetch the object with auth, then trigger a save-as via a blob URL.
  // Avoids relying on server-bind-URL-based presigned links that break
  // behind reverse proxies; the cost is buffering the file in browser
  // memory, which is acceptable for an admin tool.
  try {
    const resp = await fetch(
      BASE + '/v1/objects/' + encodeURIComponent(bucket) + '/' + encodeKey(key),
      { headers: { 'Authorization': 'Bearer ' + KEY } }
    );
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = key.split('/').pop() || 'object';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch (e) {
    showErr('Download failed: ' + (e.message || 'unknown'));
  }
}

async function deleteObject(bucket, key) {
  if (!confirm('Delete "' + key + '" from ' + bucket + '?')) return;
  try {
    const resp = await fetch(
      BASE + '/v1/objects/' + encodeURIComponent(bucket) + '/' + encodeKey(key),
      { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + KEY } }
    );
    if (!resp.ok) throw { error: 'HTTP ' + resp.status };
    loadObjects(bucket);
  } catch (e) {
    showErr('Delete failed: ' + (e.error || 'unknown'));
  }
}

function goHome() { window.location.hash = ''; }

// ---------------------------------------------------------------------------
// Modal + create/delete ops
// ---------------------------------------------------------------------------
function openModal(id) { document.getElementById(id).classList.add('active'); }
function closeModal(id) { document.getElementById(id).classList.remove('active'); }

async function createBucket() {
  const name = document.getElementById('bucketName').value.trim();
  if (!name) return;
  const pub = document.getElementById('bucketPublic').checked;
  try {
    await api('/v1/buckets', { method: 'POST', body: JSON.stringify({ name, public: pub }) });
    closeModal('createBucketModal');
    document.getElementById('bucketName').value = '';
    document.getElementById('bucketPublic').checked = false;
    await refreshStats();
    route();
  } catch (e) {
    showErr(e.error || 'Failed to create bucket');
  }
}

async function deleteBucket(name) {
  if (!confirm('Delete bucket "' + name + '"? It must be empty.')) return;
  try {
    const resp = await fetch(BASE + '/v1/buckets/' + encodeURIComponent(name), {
      method: 'DELETE',
      headers: { 'Authorization': 'Bearer ' + KEY },
    });
    if (!resp.ok) {
      const body = await resp.text();
      let msg = body;
      try { msg = JSON.parse(body).error || body; } catch (_) {}
      throw { error: msg };
    }
    await refreshStats();
    route();
  } catch (e) {
    showErr('Delete failed: ' + (e.error || 'unknown'));
  }
}

async function createKey() {
  const name = document.getElementById('keyName').value.trim();
  if (!name) return;
  const permissions = {
    read: document.getElementById('permRead').checked,
    write: document.getElementById('permWrite').checked,
    delete: document.getElementById('permDelete').checked,
    admin: document.getElementById('permAdmin').checked,
  };
  try {
    const resp = await api('/v1/admin/keys', { method: 'POST', body: JSON.stringify({ name, permissions }) });
    closeModal('createKeyModal');
    document.getElementById('keyName').value = '';
    await route();
    const alert = document.getElementById('keyAlert');
    if (alert) {
      alert.classList.remove('hidden');
      alert.textContent = 'New key (shown once): ' + resp.secret;
    }
  } catch (e) {
    showErr(e.error || 'Failed to create key');
  }
}

async function deleteKey(id) {
  if (!confirm('Revoke this API key?')) return;
  try {
    const resp = await fetch(BASE + '/v1/admin/keys/' + encodeURIComponent(id), {
      method: 'DELETE',
      headers: { 'Authorization': 'Bearer ' + KEY },
    });
    if (!resp.ok) throw { error: 'HTTP ' + resp.status };
    route();
  } catch (e) {
    showErr('Revoke failed: ' + (e.error || 'unknown'));
  }
}

async function refreshStats() {
  try { STATE.stats = await api('/v1/stats'); } catch (_) {}
}

// Auto-connect if key in URL hash
if (window.location.hash.startsWith('#key=')) {
  document.getElementById('apiKey').value = window.location.hash.slice(5);
  window.location.hash = '';
  connect();
}
</script>
</body>
</html>
"##;

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
<title>vexobj Dashboard</title>
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
  .header h1 { font-size: 20px; font-weight: 600; }
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
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; padding: 8px 12px; border-bottom: 1px solid var(--border); }
  td { padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 14px; }
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
  .mono { font-family: monospace; font-size: 13px; }
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
  .hidden { display: none; }
  .flex { display: flex; gap: 8px; }
</style>
</head>
<body>
<div class="header">
  <h1><span>Vault</span>FS</h1>
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

function api(path, opts = {}) {
  return fetch(BASE + path, {
    ...opts,
    headers: { 'Authorization': 'Bearer ' + KEY, 'Content-Type': 'application/json', ...opts.headers },
  }).then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e)));
}

async function connect() {
  KEY = document.getElementById('apiKey').value.trim();
  if (!KEY) return;
  try {
    const stats = await api('/v1/stats');
    document.getElementById('version').textContent = 'v' + stats.version;
    render(stats);
  } catch(e) {
    document.getElementById('app').innerHTML = '<div class="empty">Invalid API key or insufficient permissions (admin required)</div>';
  }
}

async function render(stats) {
  const [bucketsResp, keysResp] = await Promise.all([
    api('/v1/buckets'),
    api('/v1/admin/keys').catch(() => ({ keys: [] })),
  ]);

  const app = document.getElementById('app');
  app.innerHTML = `
    <div class="grid">
      <div class="card">
        <div class="label">Buckets</div>
        <div class="value">${stats.buckets}</div>
      </div>
      <div class="card">
        <div class="label">Objects</div>
        <div class="value">${stats.total_objects}</div>
      </div>
      <div class="card">
        <div class="label">Storage Used</div>
        <div class="value">${stats.total_size_human}</div>
        <div class="sub">Disk: ${stats.disk_usage_human}</div>
      </div>
      <div class="card">
        <div class="label">API Keys</div>
        <div class="value">${keysResp.keys.length}</div>
      </div>
    </div>

    <div class="section">
      <div class="section-title">
        <span>Buckets</span>
        <button onclick="openModal('createBucketModal')">+ New Bucket</button>
      </div>
      <div class="card">
        <table>
          <thead><tr><th>Name</th><th>Objects</th><th>Size</th><th>Created</th><th></th></tr></thead>
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
  const details = stats.bucket_details || [];
  for (const b of bucketsResp.buckets) {
    const d = details.find(x => x.name === b.name) || { objects: 0, size_human: '0 B' };
    bucketsTable.innerHTML += `<tr>
      <td><strong>${b.name}</strong></td>
      <td>${d.objects}</td>
      <td>${d.size_human}</td>
      <td class="mono">${new Date(b.created_at).toLocaleDateString()}</td>
      <td><button class="danger" onclick="deleteBucket('${b.name}')" style="padding:4px 10px;font-size:12px">Delete</button></td>
    </tr>`;
  }
  if (!bucketsResp.buckets.length) bucketsTable.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted)">No buckets yet</td></tr>';

  const keysTable = document.getElementById('keysTable');
  for (const k of keysResp.keys) {
    const perms = [];
    if (k.permissions.read) perms.push('<span class="badge green">read</span>');
    if (k.permissions.write) perms.push('<span class="badge yellow">write</span>');
    if (k.permissions.delete) perms.push('<span class="badge red">delete</span>');
    if (k.permissions.admin) perms.push('<span class="badge" style="background:rgba(99,102,241,0.15);color:var(--accent)">admin</span>');
    keysTable.innerHTML += `<tr>
      <td><strong>${k.name}</strong></td>
      <td class="mono">${k.key_prefix}...</td>
      <td>${perms.join(' ')}</td>
      <td class="mono">${new Date(k.created_at).toLocaleDateString()}</td>
      <td><button class="danger" onclick="deleteKey('${k.id}')" style="padding:4px 10px;font-size:12px">Revoke</button></td>
    </tr>`;
  }
}

function openModal(id) { document.getElementById(id).classList.add('active'); }
function closeModal(id) { document.getElementById(id).classList.remove('active'); }

async function createBucket() {
  const name = document.getElementById('bucketName').value.trim();
  if (!name) return;
  try {
    await api('/v1/buckets', { method: 'POST', body: JSON.stringify({ name, public: false }) });
    closeModal('createBucketModal');
    document.getElementById('bucketName').value = '';
    connect();
  } catch(e) { alert(e.error || 'Failed'); }
}

async function deleteBucket(name) {
  if (!confirm('Delete bucket "' + name + '"?')) return;
  try {
    await fetch(BASE + '/v1/buckets/' + name, { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + KEY } });
    connect();
  } catch(e) { alert('Failed'); }
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
    const alert = document.getElementById('keyAlert');
    alert.classList.remove('hidden');
    alert.textContent = 'New key created: ' + resp.secret + ' — Save this now, it will not be shown again.';
    connect();
  } catch(e) { alert(e.error || 'Failed'); }
}

async function deleteKey(id) {
  if (!confirm('Revoke this API key?')) return;
  try {
    await fetch(BASE + '/v1/admin/keys/' + id, { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + KEY } });
    connect();
  } catch(e) { alert('Failed'); }
}

// Auto-connect if key in URL hash
if (window.location.hash.startsWith('#key=')) {
  document.getElementById('apiKey').value = window.location.hash.slice(5);
  connect();
}
</script>
</body>
</html>
"##;

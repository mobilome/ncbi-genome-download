/* ===== NCBI Genome Manager — 共享前端逻辑 ===== */

// ── API 辅助函数 ──────────────────────────────────────────
// 所有请求统一加 8s 超时，防止服务重启时 keep-alive 僵尸连接导致无限等待
const API_TIMEOUT_MS = 8000;

async function apiGet(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(API_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`GET ${url} → ${res.status}`);
  return res.json();
}

async function apiPost(url, data) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
    signal: AbortSignal.timeout(API_TIMEOUT_MS),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `POST ${url} → ${res.status}`);
  }
  return res.json();
}

async function apiPut(url, data) {
  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
    signal: AbortSignal.timeout(API_TIMEOUT_MS),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `PUT ${url} → ${res.status}`);
  }
  return res.json();
}

async function apiDelete(url) {
  const res = await fetch(url, {
    method: 'DELETE',
    signal: AbortSignal.timeout(API_TIMEOUT_MS),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `DELETE ${url} → ${res.status}`);
  }
  return res.json();
}

// ── 格式化工具 ────────────────────────────────────────────
function fmtSize(bytes) {
  if (bytes == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let v = bytes, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return v.toFixed(i === 0 ? 0 : 1) + ' ' + units[i];
}

function fmtDate(s) {
  if (!s) return '—';
  return s.replace('T', ' ').slice(0, 19);
}

function statusBadge(status) {
  const map = {
    pending:   'badge badge-pending',
    running:   'badge badge-running',
    done:      'badge badge-done',
    failed:    'badge badge-failed',
    cancelled: 'badge badge-cancelled',
  };
  return map[status] || 'badge bg-secondary';
}

function statusIcon(status) {
  const map = {
    pending:   'bi-clock',
    running:   'bi-arrow-repeat',
    done:      'bi-check-circle-fill',
    failed:    'bi-x-circle-fill',
    cancelled: 'bi-slash-circle',
  };
  return map[status] || 'bi-question-circle';
}

// 根据日志内容中的 [LEVEL] 标签附加颜色 class
function logLineClass(content) {
  if (content.includes('[SUCCESS]')) return 'log-success';
  if (content.includes('[WARNING]')) return 'log-warning';
  if (content.includes('[ERROR]'))   return 'log-error';
  if (content.includes('[RUN]'))     return 'log-run';
  if (content.includes('[SHELL]'))   return 'log-shell';
  if (content.includes('[STEP]'))    return 'log-step';
  return 'log-info';
}

// ── Toast 通知 ────────────────────────────────────────────
function showToast(msg, type = 'success') {
  const container = document.getElementById('toast-container');
  if (!container) return;
  const id = 'toast-' + Date.now();
  const bgClass = type === 'success' ? 'text-bg-success'
                : type === 'danger'  ? 'text-bg-danger'
                : 'text-bg-warning';
  container.insertAdjacentHTML('beforeend', `
    <div id="${id}" class="toast align-items-center ${bgClass} border-0" role="alert">
      <div class="d-flex">
        <div class="toast-body">${msg}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto"
                data-bs-dismiss="toast"></button>
      </div>
    </div>`);
  const el = document.getElementById(id);
  const t = new bootstrap.Toast(el, { delay: 3500 });
  t.show();
  el.addEventListener('hidden.bs.toast', () => el.remove());
}

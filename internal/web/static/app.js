'use strict';

const $ = (id) => document.getElementById(id);
const state = {
  data: null,
  frames: null,
  page: 0,
  pageSize: 100,
  compare: null,
  inputsFilled: false,
};

function pct(x, digits = 1) {
  if (!Number.isFinite(x)) return '0.' + '0'.repeat(digits) + '%';
  return (x * 100).toFixed(digits) + '%';
}
function num(x) { return (x ?? 0).toLocaleString('zh-CN'); }
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
}
function classTag(c) {
  return `<span class="tag ${esc(c)}">${esc(c || 'unknown')}</span>`;
}
async function api(url, opts) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(`${url} HTTP ${res.status}: ${await res.text()}`);
  return res.json();
}
function toast(msg, isError = false) {
  const el = $('toast');
  el.textContent = msg;
  el.className = 'toast show' + (isError ? ' error' : '');
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { el.className = 'toast'; }, 3200);
}

function setupCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const w = Math.max(10, rect.width);
  const h = Math.max(10, rect.height);
  canvas.width = Math.round(w * dpr);
  canvas.height = Math.round(h * dpr);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  return { ctx, w, h };
}

function drawAxes(ctx, w, h, pad) {
  ctx.strokeStyle = '#263653';
  ctx.lineWidth = 1;
  ctx.fillStyle = '#91a4c7';
  ctx.font = '11px ui-monospace, Menlo, monospace';
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (h - pad.top - pad.bottom) * i / 4;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();
    const v = 1 - i / 4;
    ctx.fillText((v * 100).toFixed(0) + '%', pad.left - 7, y);
  }
  ctx.textAlign = 'center';
  ctx.fillText('时间 / 访问序号 →', w / 2, h - 8);
}

function drawLineChart(canvas, series, options = {}) {
  const { ctx, w, h } = setupCanvas(canvas);
  const pad = { left: 44, right: 14, top: 14, bottom: 30 };
  drawAxes(ctx, w, h, pad);
  const iw = w - pad.left - pad.right;
  const ih = h - pad.top - pad.bottom;
  const maxX = options.maxX || Math.max(1, ...series.flatMap(s => s.data.map(d => d.x ?? 0)));

  for (const s of series) {
    if (!s.data || s.data.length === 0) continue;
    ctx.strokeStyle = s.color;
    ctx.lineWidth = s.width || 2;
    ctx.beginPath();
    s.data.forEach((d, i) => {
      const x = pad.left + ((d.x ?? i) / maxX) * iw;
      const y = pad.top + (1 - Math.max(0, Math.min(1, d.y))) * ih;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();

    if (s.fill) {
      ctx.lineTo(pad.left + ((s.data[s.data.length - 1].x ?? s.data.length) / maxX) * iw, pad.top + ih);
      ctx.lineTo(pad.left, pad.top + ih);
      ctx.closePath();
      ctx.fillStyle = s.fill;
      ctx.fill();
    }
  }
}

function renderKPIs(s) {
  const m = s.metrics;
  const reqRate = m.requests ? m.hits / m.requests : 0;
  const policyRate = m.policyAccesses ? m.policyHits / m.policyAccesses : 0;
  const cards = [
    ['总请求命中率', pct(reqRate, 2), `${num(m.hits)} / ${num(m.requests)}`, reqRate > .8 ? 'good' : reqRate > .5 ? 'warn' : 'bad'],
    ['去重后有效命中率', pct(policyRate, 2), `折叠同事务重复 ${num(m.duplicateTxAccesses)}`, 'accent'],
    ['缓存帧 / 目标', `${num(m.frames)} / ${num(m.capacityTarget)}`, `heap ${num(s.heapSize)} 页`, ''],
    ['磁盘读 / 写', `${num(s.disk.reads)} / ${num(s.disk.writes)}`, `淘汰 ${num(m.evictions)}，脏淘汰 ${num(m.dirtyEvictions)}`, ''],
    ['脏页 / 刷盘队列', `${num(m.dirtyFrames)} / ${num(m.flushQueue)}`, `已刷 ${num(m.flushWrites)}，后台 ${num(m.backgroundFlush)}`, m.dirtyFrames ? 'warn' : 'good'],
    ['幽灵历史', num(m.ghostEntries), `短时间读回 ${num(m.ghostRevivals)}，过期 ${num(m.ghostExpirations)}`, 'purple'],
    ['预读', `发 ${num(m.prefetchIssued)}`, `命中 ${num(m.prefetchHits)} / 用到 ${num(m.prefetchUsed)} / 浪费 ${num(m.prefetchWasted)}`, 'accent'],
    ['缩容主动淘汰', num(m.shrinkEvictions), `当前 LSN ${num(m.currentLsn)}`, 'warn'],
  ];
  $('kpis').innerHTML = cards.map(([label, value, hint, cls]) => `
    <div class="kpi">
      <div class="label">${label}</div>
      <div class="value ${cls}">${value}</div>
      <div class="hint">${hint}</div>
    </div>`).join('');
}

function renderCandidate(s) {
  const c = s.candidate;
  if (!c) {
    $('candidate').innerHTML = '<div class="small">池子为空。</div>';
    return;
  }
  const f = c.frame;
  $('candidateTier').textContent = `tier ${f.priorityTier} · ${f.segment}`;
  const hist = (f.recentAgesMs || []).map(a => `<span>${num(a)} ms</span>`).join('');
  $('candidate').innerHTML = `
    <div class="candidate-id">#${f.id}</div>
    <div class="reason">${esc(c.reason)}</div>
    <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px">
      ${classTag(f.queryType)}
      <span class="tag ${f.segment}">${f.segment === 'young' ? '少段 young' : '老段 old'}</span>
      ${f.dirty ? '<span class="tag dirty">dirty 将先刷盘</span>' : ''}
      ${f.prefetch ? `<span class="tag prefetch">prefetch${f.prefetchUsed ? ' 已用' : ' 未用'}</span>` : ''}
      <span class="tag candidate">heap root</span>
    </div>
    <div class="small">有效访问 ${num(f.accessCount)} 次；入池 ${num(f.ageMs)}ms；优先级键年龄 ${num(f.priorityKeyAgeMs)}ms</div>
    <div class="small" style="margin:10px 0 6px">最近访问时间（距今）</div>
    <div class="history">${hist || '<span>尚无有效访问</span>'}</div>
  `;
}

function renderUsage(s) {
  const usage = s.usage || [];
  const max = Math.max(1, ...usage.map(u => u.frames));
  const total = usage.reduce((a, u) => a + u.frames, 0);
  const colors = { hotspot: '#fb7185', scan: '#a78bfa', random: '#38bdf8', mixed: '#fbbf24', unknown: '#94a3b8', verification: '#34d399' };
  $('usage').innerHTML = usage.length ? usage.map(u => {
    const width = (u.frames / max * 100).toFixed(1);
    const share = total ? (u.frames / total * 100).toFixed(1) : '0.0';
    return `<div class="usage-row">
      <div>${classTag(u.queryType)}</div>
      <div class="bar"><i style="width:${width}%;background:${colors[u.queryType] || '#34d399'}"></i></div>
      <div class="usage-meta">${num(u.frames)} 页 · ${share}%<br>脏 ${u.dirtyFrames} / 预读 ${u.prefetchFrames}（未用 ${u.unusedPrefetch}）</div>
    </div>`;
  }).join('') : '<div class="small">暂无缓存页。</div>';
}

function renderDirty(s) {
  const q = s.flushQueue || [];
  $('dirtySummary').textContent = `${num(s.metrics.dirtyFrames)} dirty · ${num(q.length)} queued`;
  $('dirtyRows').innerHTML = q.slice(0, 250).map(j => `
    <tr>
      <td>${j.position}</td>
      <td class="mono">#${j.pageId}</td>
      <td>${num(j.dirtyAgeMs)} ms</td>
      <td class="mono">${num(j.writeLsn)}</td>
      <td>${classTag(j.queryType)}</td>
    </tr>`).join('') || '<tr><td colspan="5" class="small">没有脏页，刷盘队列为空。</td></tr>';

  const g = s.ghosts || [];
  $('ghostSummary').textContent = `${num(s.metrics.ghostEntries)} retained · showing ${g.length}`;
  $('ghostRows').innerHTML = g.map(x => `
    <tr>
      <td class="mono">#${x.pageId}</td>
      <td>${x.accessCount}</td>
      <td>${num(x.evictedAgoMs)}ms 前</td>
      <td class="mono wrap">${(x.historyAgesMs || []).slice(0, 8).map(a => num(a)).join(', ')}</td>
    </tr>`).join('') || '<tr><td colspan="4" class="small">暂无幽灵记录。</td></tr>';
}

async function loadFrames() {
  const params = new URLSearchParams({
    offset: String(state.page * state.pageSize),
    limit: String(state.pageSize),
    q: $('frameSearch').value.trim(),
    class: $('classFilter').value,
    segment: $('segmentFilter').value,
  });
  try {
    state.frames = await api('/api/frames?' + params);
    renderFrames();
  } catch (e) { toast(e.message, true); }
}

function renderFrames() {
  const f = state.frames;
  if (!f) return;
  const rows = f.items.map(x => {
    const ages = (x.recentAgesMs || []).slice(0, 8);
    const marks = [
      x.dirty ? '<span class="tag dirty">dirty</span>' : '',
      x.prefetch ? `<span class="tag prefetch">prefetch${x.prefetchUsed ? '✓' : '!'}</span>` : '',
      x.candidate ? '<span class="tag candidate">candidate</span>' : '',
    ].join(' ');
    return `<tr>
      <td class="mono">${x.candidate ? '▶ ' : ''}#${x.id}</td>
      <td>${classTag(x.queryType)}</td>
      <td><span class="tag ${x.segment}">${x.segment}</span></td>
      <td>${x.accessCount}</td>
      <td class="mono wrap">${ages.length ? ages.map(a => num(a) + 'ms').join(' · ') : '—'}</td>
      <td>${marks}</td>
      <td class="mono">${num(x.writeLsn)} / v${x.version}</td>
    </tr>`;
  }).join('');
  $('frameRows').innerHTML = rows || '<tr><td colspan="7" class="small">没有匹配页。</td></tr>';
  const pages = Math.max(1, Math.ceil(f.total / state.pageSize));
  $('pageInfo').textContent = `第 ${state.page + 1}/${pages} 页 · 共 ${num(f.total)} 页`;
  $('prevPage').disabled = state.page <= 0;
  $('nextPage').disabled = state.page + 1 >= pages;
}

async function loadState() {
  try {
    const s = await api('/api/state');
    state.data = s;
    if (!state.inputsFilled) fillConfig(s.config);
    renderKPIs(s);
    renderCandidate(s);
    renderUsage(s);
    renderDirty(s);
    $('healthPill').textContent = '● 混合负载运行中';
    $('healthPill').classList.remove('bad');
  } catch (e) {
    $('healthPill').textContent = '● 连接失败';
    $('healthPill').classList.add('bad');
  }
}

function fillConfig(c) {
  $('capacity').value = c.capacity;
  $('k').value = c.k;
  $('dedup').value = c.dedupWindowMs;
  $('ghostTtl').value = c.ghostTtlMs;
  $('oldAge').value = c.oldMinResidencyMs;
  $('prefetchDepth').value = c.prefetchDepth;
  $('seqThreshold').value = c.sequentialThreshold;
  $('prefetchTtl').value = c.prefetchUnusedTtlMs;
  state.inputsFilled = true;
}

async function loadSeries() {
  try {
    const rows = await api('/api/timeseries');
    const t0 = rows[0]?.elapsedMs || 0;
    drawLineChart($('hitChart'), [
      { color: '#38bdf8', width: 2, data: rows.map(r => ({ x: r.elapsedMs - t0, y: r.windowHitRate })) },
      { color: '#34d399', width: 2, data: rows.map(r => ({ x: r.elapsedMs - t0, y: r.cumulativeHitRate })) },
    ], { maxX: Math.max(1, ...rows.map(r => r.elapsedMs - t0)) });
    const last = rows[rows.length - 1];
    $('sampleHint').textContent = last ? `最近 0.5s ${num(last.windowHits)}/${num(last.windowRequests)} 请求；池 ${last.frames} 页` : '等待采样…';
  } catch (e) { /* polling will retry */ }
}

const compareColors = {
  'OPT': '#fbbf24',
  'LRU-K': '#fb7185',
  'LRU': '#38bdf8',
  'Clock': '#a78bfa',
};
function compareColor(name) {
  for (const k of Object.keys(compareColors)) if (name.includes(k)) return compareColors[k];
  return '#34d399';
}

async function loadCompare() {
  try {
    state.compare = await api('/api/compare');
    renderCompare();
  } catch (e) { toast(e.message, true); }
}
function renderCompare() {
  const c = state.compare;
  if (!c) return;
  const maxX = Math.max(1, ...c.results.flatMap(r => r.series.map(p => p.x)));
  drawLineChart($('compareChart'), c.results.map(r => ({
    color: compareColor(r.name),
    width: r.name.includes('OPT') ? 3 : 2,
    data: r.series.map(p => ({ x: p.x, y: p.cumulativeRate })),
  })), { maxX });

  const opt = c.results.find(r => r.name.includes('OPT'));
  $('compareTable').innerHTML = `
    <div class="small" style="margin-bottom:8px">
      seed=${c.seed} · ${num(c.rawEvents)} events → ${num(c.effectiveRefs)} effective refs · capacity=${c.capacity} · K=${c.k}
      <br><span class="${c.optUpperBoundOk ? 'good' : 'bad'}">OPT 上界校验：${c.optUpperBoundOk ? '通过（所有算法 ≤ OPT）' : '异常'}</span>
    </div>
    ${c.results.map(r => {
      const gap = opt ? (opt.hitRate - r.hitRate) * 100 : 0;
      return `<div class="metric-line">
        <span><i style="display:inline-block;width:9px;height:9px;background:${compareColor(r.name)};border-radius:2px;margin-right:6px"></i>${esc(r.name)}</span>
        <strong>${pct(r.hitRate, 2)}</strong>
      </div>
      <div class="small" style="margin:-3px 0 7px 15px">hits ${num(r.hits)} / ${num(r.references)}，距 OPT ${gap >= 0 ? gap.toFixed(2) + ' pp' : '高于 OPT!'}</div>`;
    }).join('')}`;
}

function verifyText(r) {
  const lines = [];
  lines.push(`总体结果: ${r.ok ? '✅ 全部对上' : '❌ 存在不一致'}`);
  lines.push(`Trace: seed=${r.workload.seed}, pages=${r.workload.pageCount}, events=${r.workload.events}`);
  lines.push(`策略上界: OPT=${pct(r.comparison.optHitRate, 3)}, LRU-K=${pct(r.comparison.lrukHitRate, 3)}, LRU=${pct(r.comparison.lruHitRate, 3)}, Clock=${pct(r.comparison.clockHitRate, 3)}, boundOK=${r.comparison.optUpperBoundOk}`);
  lines.push('');
  lines.push(`写入 Oracle: ${r.consistency.writtenPages} 个写过的页, ${r.consistency.writeObservations} 次写提交`);
  lines.push(`磁盘校验: 检查 ${r.consistency.checkedDiskPages} 页 checksum, 比对 last-write ${r.consistency.checkedWrites} 页`);
  lines.push(`一致性: ok=${r.consistency.ok}, mismatches=${(r.consistency.mismatches || []).length}`);
  lines.push(`干净缓存帧 invariant: checked=${r.cleanCheck.checkedFrames}, dirtySkipped=${r.cleanCheck.dirtySkipped}, ok=${r.cleanCheck.ok}`);
  lines.push(`全新池读回全磁盘: pages=${r.readThrough.pages}, ok=${r.readThrough.ok}`);
  lines.push('');
  lines.push(`Pool requests=${r.poolMetrics.requests}, hits=${r.poolMetrics.hits}, evictions=${r.poolMetrics.evictions}, dirtyEvictions=${r.poolMetrics.dirtyEvictions}`);
  lines.push(`Prefetch issued=${r.poolMetrics.prefetchIssued}, hits=${r.poolMetrics.prefetchHits}, wasted=${r.poolMetrics.prefetchWasted}`);
  if ((r.consistency.mismatches || []).length) lines.push('\nMISMATCHES:\n' + r.consistency.mismatches.join('\n'));
  if ((r.errors || []).length) lines.push('\nERRORS:\n' + r.errors.join('\n'));
  return lines.join('\n');
}
async function loadVerify(refresh = false) {
  const btn = $('rerunVerify');
  btn.disabled = true;
  try {
    const r = await api('/api/verify' + (refresh ? '?refresh=1&events=36000' : ''));
    $('verifySummary').innerHTML = r.ok
      ? '<strong class="good">✅ 字节级一致性通过：所有最后写入均与磁盘一致，OPT 上界成立。</strong>'
      : '<strong class="bad">❌ 校验失败，请查看报告</strong>';
    $('verifyReport').textContent = verifyText(r);
  } catch (e) {
    $('verifySummary').innerHTML = '<strong class="bad">校验请求失败</strong>';
    $('verifyReport').textContent = e.message;
  } finally {
    btn.disabled = false;
  }
}

async function applyConfig() {
  const patch = {
    k: Number($('k').value),
    dedupWindowMs: Number($('dedup').value),
    ghostTtlMs: Number($('ghostTtl').value),
    oldMinResidencyMs: Number($('oldAge').value),
    prefetchDepth: Number($('prefetchDepth').value),
    sequentialThreshold: Number($('seqThreshold').value),
    prefetchUnusedTtlMs: Number($('prefetchTtl').value),
  };
  try {
    const cfg = await api('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(patch) });
    fillConfig(cfg);
    toast('配置已应用；修改 K 时堆已一次性重建');
    loadCompare();
  } catch (e) { toast(e.message, true); }
}
async function resize() {
  const capacity = Number($('capacity').value);
  try {
    const cfg = await api('/api/capacity', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ capacity }) });
    fillConfig(cfg);
    toast(`目标容量改为 ${cfg.capacity}；缩容将按淘汰规则分批完成`);
  } catch (e) { toast(e.message, true); }
}

$('applyConfig').addEventListener('click', applyConfig);
$('resizeBtn').addEventListener('click', resize);
$('filterBtn').addEventListener('click', () => { state.page = 0; loadFrames(); });
$('prevPage').addEventListener('click', () => { state.page = Math.max(0, state.page - 1); loadFrames(); });
$('nextPage').addEventListener('click', () => { state.page++; loadFrames(); });
$('pageSize').addEventListener('change', (e) => { state.pageSize = Number(e.target.value); state.page = 0; loadFrames(); });
$('frameSearch').addEventListener('keydown', (e) => { if (e.key === 'Enter') { state.page = 0; loadFrames(); } });
$('rerunVerify').addEventListener('click', () => loadVerify(true));
$('liveCheck').addEventListener('click', async () => {
  try {
    const r = await api('/api/verify?live=1');
    toast(r.ok ? `干净页校验通过：${r.checkedFrames} 帧与磁盘一致` : `发现 ${r.mismatches.length} 个不一致`, !r.ok);
  } catch (e) { toast(e.message, true); }
});
window.addEventListener('resize', () => { loadSeries(); if (state.compare) renderCompare(); });

loadState();
loadFrames();
loadSeries();
loadCompare();
loadVerify(false);
setInterval(loadState, 1000);
setInterval(loadFrames, 2500);
setInterval(loadSeries, 1000);

import os
import re
import subprocess
import threading
import time
import io

from flask import Flask, request, jsonify, send_file, Response

from hive import db

app = Flask(__name__)

# Set by run_coordinator()
PDF_SOURCE = ""
TEXT_DEST = ""
STALE_MINUTES = 10

# Parsed source/dest info
_src = {}  # {"type": "local"|"ssh", "path": ..., "user": ..., "host": ...}
_dst = {}


def _parse_location(loc: str) -> dict:
    """Parse 'ssh://user@host:/path' or '/local/path'."""
    m = re.match(r"ssh://([^@]+)@([^:]+):(.+)", loc)
    if m:
        return {"type": "ssh", "user": m.group(1), "host": m.group(2), "path": m.group(3)}
    return {"type": "local", "path": loc}


def _ssh_cmd(info: dict, cmd: str) -> str:
    """Run a command via SSH and return stdout."""
    result = subprocess.run(
        ["ssh", f"{info['user']}@{info['host']}", cmd],
        capture_output=True, text=True, timeout=120,
    )
    return result.stdout


def _ssh_read_file(info: dict, remote_path: str) -> bytes:
    """Read a file from remote via SSH."""
    result = subprocess.run(
        ["ssh", f"{info['user']}@{info['host']}", "cat", f'"{remote_path}"'],
        capture_output=True, timeout=STALE_MINUTES * 60,
    )
    return result.stdout


def _ssh_write_file(info: dict, remote_path: str, data: bytes):
    """Write data to a remote file via SSH."""
    remote_dir = os.path.dirname(remote_path)
    subprocess.run(
        ["ssh", f"{info['user']}@{info['host']}", "mkdir", "-p", f'"{remote_dir}"'],
        capture_output=True, timeout=30,
    )
    subprocess.run(
        ["ssh", f"{info['user']}@{info['host']}", f'cat > "{remote_path}"'],
        input=data, capture_output=True, timeout=120,
    )


def _scan_pdfs() -> list[tuple[str, str]]:
    """Scan PDF source for all PDF files. Returns [(pdf_path, text_path), ...]."""
    print("[coordinator] Scanning for PDFs...")

    if _src["type"] == "local":
        import glob
        pdfs = glob.glob(os.path.join(_src["path"], "**", "*.pdf"), recursive=True)
    else:
        raw = _ssh_cmd(_src, f'find "{_src["path"]}" -name "*.pdf" -type f')
        pdfs = [p.strip() for p in raw.strip().split("\n") if p.strip()]

    print(f"[coordinator] Found {len(pdfs)} PDFs")

    # Build text output paths
    pairs = []
    for pdf in pdfs:
        # Relative path from source root
        if _src["type"] == "local":
            rel = os.path.relpath(pdf, _src["path"])
        else:
            rel = os.path.relpath(pdf, _src["path"])

        text_rel = os.path.splitext(rel)[0] + ".txt"

        if _dst["type"] == "local":
            text_path = os.path.join(_dst["path"], text_rel)
        else:
            text_path = os.path.join(_dst["path"], text_rel)

        pairs.append((pdf, text_path))

    return pairs


def _skip_existing(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Filter out PDFs that already have text output."""
    if _dst["type"] == "local":
        return [(p, t) for p, t in pairs if not os.path.exists(t)]
    else:
        # Get list of existing text files via SSH
        print("[coordinator] Checking for existing text files...")
        raw = _ssh_cmd(_dst, f'find "{_dst["path"]}" -name "*.txt" -type f')
        existing = {p.strip() for p in raw.strip().split("\n") if p.strip()}
        return [(p, t) for p, t in pairs if t not in existing]


# ──────────────────────────── API Routes ────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "uptime": time.time() - _start_time})


@app.route("/workers/register", methods=["POST"])
def register_worker():
    data = request.json
    db.register_worker(
        name=data["name"],
        ip=request.remote_addr,
        cores=data.get("cores", 0),
    )
    return jsonify({"status": "registered"})


@app.route("/workers")
def list_workers():
    return jsonify(db.get_workers())


@app.route("/workers/stats", methods=["POST"])
def worker_stats():
    data = request.json
    db.update_worker_stats(data["name"], data["stats"])
    return jsonify({"status": "ok"})


@app.route("/tasks/pull", methods=["POST"])
def pull_tasks():
    data = request.json
    tasks = db.pull_tasks(
        worker=data["worker"],
        batch_size=data.get("batch_size", 50),
    )
    db.heartbeat_worker(data["worker"])
    return jsonify(tasks)


@app.route("/tasks/report", methods=["POST"])
def report_tasks():
    data = request.json
    db.report_results(data["results"])
    return jsonify({"status": "ok", "count": len(data["results"])})


@app.route("/tasks/stats")
def task_stats():
    stats = db.get_stats()
    rate_info = db.get_rate_info()
    stats.update(rate_info)
    stats["workers"] = db.get_workers()
    return jsonify(stats)


@app.route("/api/stats")
def api_stats():
    """Alias for dashboard."""
    return task_stats()


@app.route("/files/<int:task_id>")
def download_file(task_id):
    """Proxy a PDF to the worker."""
    pdf_path = db.get_task_path(task_id)
    if not pdf_path:
        return jsonify({"error": "task not found"}), 404

    if _src["type"] == "local":
        if os.path.exists(pdf_path):
            return send_file(pdf_path, mimetype="application/pdf")
        return jsonify({"error": "file not found"}), 404
    else:
        data = _ssh_read_file(_src, pdf_path)
        if not data:
            return jsonify({"error": "file not found"}), 404
        return Response(data, mimetype="application/pdf")


@app.route("/files/upload/<int:task_id>", methods=["POST"])
def upload_file(task_id):
    """Receive extracted text from a worker."""
    text_path = db.get_task_text_path(task_id)
    if not text_path:
        return jsonify({"error": "task not found"}), 404

    data = request.get_data()

    if _dst["type"] == "local":
        os.makedirs(os.path.dirname(text_path), exist_ok=True)
        with open(text_path, "wb") as f:
            f.write(data)
    else:
        _ssh_write_file(_dst, text_path, data)

    return jsonify({"status": "ok"})


# ──────────────────────────── Dashboard ────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Hive Dashboard</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:#0a0a0f; color:#e0e0e0; font-family:'SF Mono','Cascadia Code',monospace; padding:24px; }
  h1 { font-size:20px; color:#7c6ff7; margin-bottom:20px; }
  .cards { display:flex; gap:16px; margin-bottom:20px; flex-wrap:wrap; }
  .card { background:#14141f; border:1px solid #222; border-radius:10px; padding:16px 24px; min-width:140px; }
  .card .value { font-size:28px; font-weight:bold; color:#fff; }
  .card .label { font-size:11px; color:#888; text-transform:uppercase; margin-top:4px; }
  .progress-bar { background:#14141f; border-radius:8px; height:28px; margin-bottom:20px; overflow:hidden; border:1px solid #222; position:relative; }
  .progress-fill { height:100%; background:linear-gradient(90deg,#7c6ff7,#a78bfa); transition:width 0.5s; border-radius:8px; }
  .progress-text { position:absolute; right:12px; top:5px; font-size:13px; color:#fff; }
  table { width:100%; border-collapse:collapse; }
  th { text-align:left; padding:8px 12px; color:#888; font-size:11px; text-transform:uppercase; border-bottom:1px solid #222; }
  td { padding:8px 12px; border-bottom:1px solid #1a1a2a; font-size:13px; }
  .status-working { color:#4ade80; }
  .status-stale { color:#f87171; }
  .section { background:#14141f; border:1px solid #222; border-radius:10px; padding:16px; margin-bottom:20px; }
  .section h2 { font-size:13px; color:#888; margin-bottom:12px; text-transform:uppercase; }
  .sparkline { width:100%; height:60px; }
  .footer { font-size:11px; color:#555; margin-top:12px; }
  .methods span { display:inline-block; margin-right:16px; }
  .methods .pct { color:#7c6ff7; }
  .bar-bg { background:#1a1a2a; border-radius:4px; height:8px; width:100px; display:inline-block; vertical-align:middle; overflow:hidden; }
  .bar-fill { height:100%; border-radius:4px; transition:width 0.5s; }
  .bar-cpu { background:linear-gradient(90deg,#4ade80,#f59e0b,#ef4444); }
  .bar-ram { background:linear-gradient(90deg,#60a5fa,#a78bfa); }
  .temp { font-size:12px; }
  .temp-ok { color:#4ade80; }
  .temp-warm { color:#f59e0b; }
  .temp-hot { color:#ef4444; }
  .gpu-badge { background:#1e1e3a; border:1px solid #333; border-radius:4px; padding:1px 6px; font-size:11px; color:#a78bfa; }
</style>
</head>
<body>
<h1>HIVE &mdash; Distributed Task Monitor</h1>

<div class="cards">
  <div class="card"><div class="value" id="total">-</div><div class="label">Total</div></div>
  <div class="card"><div class="value" id="done">-</div><div class="label">Done</div></div>
  <div class="card"><div class="value" id="rate">-</div><div class="label">Speed (/s)</div></div>
  <div class="card"><div class="value" id="eta">-</div><div class="label">ETA</div></div>
</div>

<div class="progress-bar">
  <div class="progress-fill" id="pbar" style="width:0%"></div>
  <div class="progress-text" id="ppct">0%</div>
</div>

<div class="section">
  <h2>Workers</h2>
  <table>
    <thead><tr>
      <th>Worker</th><th>Cores</th><th>CPU</th><th>RAM</th><th>Temp</th><th>GPU</th>
      <th>Done</th><th>Failed</th><th>Status</th>
    </tr></thead>
    <tbody id="workers"></tbody>
  </table>
</div>

<div class="section">
  <h2>Task Breakdown</h2>
  <div id="breakdown"></div>
  <div class="methods" id="methods" style="margin-top:8px;"></div>
</div>

<div class="section">
  <h2>Rate History</h2>
  <svg class="sparkline" id="spark" viewBox="0 0 600 60" preserveAspectRatio="none"></svg>
</div>

<div class="footer">Auto-refresh: 5s &nbsp; <button onclick="paused=!paused;this.textContent=paused?'Resume':'Pause'" style="background:#222;color:#aaa;border:1px solid #333;padding:2px 10px;border-radius:4px;cursor:pointer;">Pause</button></div>

<script>
let paused = false;

function fmt(n) {
  if (n === undefined || n === null) return '-';
  return n.toLocaleString();
}

function fmtEta(secs) {
  if (!secs || secs <= 0) return '-';
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  return h > 0 ? h + 'h ' + m + 'm' : m + 'm';
}

function tempClass(t) {
  if (t === null || t === undefined) return 'temp-ok';
  if (t < 60) return 'temp-ok';
  if (t < 80) return 'temp-warm';
  return 'temp-hot';
}

function bar(pct, cls) {
  const p = Math.min(100, Math.max(0, pct || 0));
  return '<div class="bar-bg"><div class="bar-fill '+cls+'" style="width:'+p+'%"></div></div> '+Math.round(p)+'%';
}

async function refresh() {
  if (paused) return;
  try {
    const r = await fetch('/api/stats');
    const d = await r.json();

    document.getElementById('total').textContent = fmt(d.total);
    document.getElementById('done').textContent = fmt(d.done);
    document.getElementById('rate').textContent = d.rate_per_sec || '0';
    document.getElementById('eta').textContent = fmtEta(d.eta_seconds);

    const pct = d.total > 0 ? (d.done / d.total * 100) : 0;
    document.getElementById('pbar').style.width = pct + '%';
    document.getElementById('ppct').textContent = pct.toFixed(1) + '%';

    // Workers with system stats
    const wb = document.getElementById('workers');
    wb.innerHTML = '';
    (d.workers || []).forEach(w => {
      const stale = (Date.now()/1000 - w.last_seen) > 60;
      const cls = stale ? 'status-stale' : 'status-working';
      const label = stale ? 'stale' : 'working';

      const ramPct = w.ram_total_gb > 0 ? (w.ram_used_gb / w.ram_total_gb * 100) : 0;
      const ramStr = bar(ramPct, 'bar-ram') + ' <span style="color:#666;font-size:11px;">'+
        (w.ram_used_gb||0).toFixed(1)+'/'+( w.ram_total_gb||0).toFixed(0)+'G</span>';

      const cpuStr = bar(w.cpu_pct, 'bar-cpu');

      let tempStr = '-';
      if (w.cpu_temp !== null && w.cpu_temp !== undefined) {
        tempStr = '<span class="temp '+tempClass(w.cpu_temp)+'">'+Math.round(w.cpu_temp)+'&deg;C</span>';
      }

      let gpuStr = '-';
      if (w.gpu_pct !== null && w.gpu_pct !== undefined) {
        gpuStr = '<span class="gpu-badge">'+Math.round(w.gpu_pct)+'%</span>';
        if (w.gpu_temp !== null && w.gpu_temp !== undefined) {
          gpuStr += ' <span class="temp '+tempClass(w.gpu_temp)+'">'+Math.round(w.gpu_temp)+'&deg;C</span>';
        }
      }

      wb.innerHTML += '<tr><td>'+w.name+'</td><td>'+w.cores+'</td>'+
        '<td>'+cpuStr+'</td><td>'+ramStr+'</td><td>'+tempStr+'</td><td>'+gpuStr+'</td>'+
        '<td>'+fmt(w.tasks_completed)+'</td><td>'+w.tasks_failed+'</td>'+
        '<td class="'+cls+'">'+label+'</td></tr>';
    });

    // Breakdown
    document.getElementById('breakdown').textContent =
      'pending: '+fmt(d.pending)+'  assigned: '+fmt(d.assigned)+'  failed: '+fmt(d.failed);

    // Methods
    const mEl = document.getElementById('methods');
    mEl.innerHTML = '';
    const mTotal = Object.values(d.methods||{}).reduce((a,b)=>a+b, 0) || 1;
    for (const [k,v] of Object.entries(d.methods||{})) {
      mEl.innerHTML += '<span>'+k+': <span class="pct">'+Math.round(v/mTotal*100)+'%</span> ('+fmt(v)+')</span>';
    }

    // Sparkline
    const hist = d.history || [];
    if (hist.length > 1) {
      const max = Math.max(...hist, 1);
      const w = 600, h = 60;
      const step = w / (hist.length - 1);
      let pts = hist.map((v, i) => (i*step)+','+(h - v/max*h)).join(' ');
      document.getElementById('spark').innerHTML =
        '<polyline points="'+pts+'" fill="none" stroke="#7c6ff7" stroke-width="2"/>';
    }
  } catch(e) {}
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


@app.route("/")
def dashboard():
    return DASHBOARD_HTML


# ──────────────────────────── Background Threads ────────────────────────────

_start_time = time.time()


def _stale_recovery_loop(minutes: int):
    while True:
        time.sleep(60)
        try:
            n = db.recover_stale(minutes)
            if n > 0:
                print(f"[coordinator] Recovered {n} stale tasks")
        except Exception as e:
            print(f"[coordinator] Stale recovery error: {e}")


def _rate_log_loop():
    while True:
        time.sleep(10)
        try:
            db.log_rate()
        except Exception as e:
            print(f"[coordinator] Rate log error: {e}")


# ──────────────────────────── Entry Point ────────────────────────────

def run_coordinator(port: int, pdf_source: str, text_dest: str, stale_minutes: int):
    global PDF_SOURCE, TEXT_DEST, STALE_MINUTES, _src, _dst

    PDF_SOURCE = pdf_source
    TEXT_DEST = text_dest
    STALE_MINUTES = stale_minutes

    _src = _parse_location(pdf_source)
    _dst = _parse_location(text_dest)

    print(f"[coordinator] PDF source: {_src}")
    print(f"[coordinator] Text dest:  {_dst}")

    # Init database
    db.init_db()

    # Scan and load tasks
    pairs = _scan_pdfs()
    new_pairs = _skip_existing(pairs)
    print(f"[coordinator] {len(pairs)} total PDFs, {len(pairs) - len(new_pairs)} already done")

    if new_pairs:
        added = db.add_tasks(new_pairs)
        print(f"[coordinator] Added {added} new tasks to queue")

    stats = db.get_stats()
    print(f"[coordinator] Queue: {stats['total']} total, {stats['pending']} pending, {stats['done']} done")

    # Start background threads
    threading.Thread(target=_stale_recovery_loop, args=(stale_minutes,), daemon=True).start()
    threading.Thread(target=_rate_log_loop, daemon=True).start()

    print(f"[coordinator] Starting on port {port}")
    print(f"[coordinator] Dashboard: http://0.0.0.0:{port}/")
    app.run(host="0.0.0.0", port=port, threaded=True)

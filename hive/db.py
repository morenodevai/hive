import sqlite3
import time
import threading

DB_PATH = "hive.db"
_local = threading.local()


def _conn() -> sqlite3.Connection:
    """One connection per thread (SQLite requirement)."""
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(DB_PATH, timeout=30)
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db():
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            pdf_path TEXT UNIQUE,
            text_path TEXT,
            status TEXT DEFAULT 'pending',
            worker TEXT,
            assigned_at REAL,
            completed_at REAL,
            method TEXT,
            char_count INTEGER,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

        CREATE TABLE IF NOT EXISTS workers (
            name TEXT PRIMARY KEY,
            ip TEXT,
            cores INTEGER,
            last_seen REAL,
            tasks_completed INTEGER DEFAULT 0,
            tasks_failed INTEGER DEFAULT 0,
            cpu_pct REAL DEFAULT 0,
            ram_used_gb REAL DEFAULT 0,
            ram_total_gb REAL DEFAULT 0,
            gpu_pct REAL,
            gpu_temp REAL,
            cpu_temp REAL
        );

        CREATE TABLE IF NOT EXISTS rate_log (
            timestamp REAL PRIMARY KEY,
            completed_count INTEGER
        );
    """)
    c.commit()


def add_tasks(pdf_text_pairs: list[tuple[str, str]]) -> int:
    """Bulk insert tasks. Returns number of new tasks added."""
    c = _conn()
    added = 0
    for batch_start in range(0, len(pdf_text_pairs), 500):
        batch = pdf_text_pairs[batch_start:batch_start + 500]
        cursor = c.executemany(
            "INSERT OR IGNORE INTO tasks (pdf_path, text_path) VALUES (?, ?)",
            batch,
        )
        added += cursor.rowcount
    c.commit()
    return added


def total_tasks() -> int:
    return _conn().execute("SELECT COUNT(*) FROM tasks").fetchone()[0]


def pull_tasks(worker: str, batch_size: int) -> list[dict]:
    """Atomically assign a batch of pending tasks to a worker."""
    c = _conn()
    now = time.time()
    rows = c.execute(
        "SELECT id, pdf_path, text_path FROM tasks WHERE status='pending' LIMIT ?",
        (batch_size,),
    ).fetchall()
    if not rows:
        return []

    ids = [r["id"] for r in rows]
    placeholders = ",".join("?" * len(ids))
    c.execute(
        f"UPDATE tasks SET status='assigned', worker=?, assigned_at=? "
        f"WHERE id IN ({placeholders})",
        [worker, now] + ids,
    )
    c.commit()
    return [{"task_id": r["id"], "pdf_path": r["pdf_path"], "text_path": r["text_path"]} for r in rows]


def report_results(results: list[dict]):
    """Mark tasks as done or failed. Update worker stats."""
    c = _conn()
    now = time.time()
    done_count = 0
    fail_count = 0
    worker_name = None

    for r in results:
        worker_name = r.get("worker", worker_name)
        if r["status"] == "done":
            c.execute(
                "UPDATE tasks SET status='done', completed_at=?, method=?, "
                "char_count=?, error=NULL WHERE id=?",
                (now, r.get("method"), r.get("char_count"), r["task_id"]),
            )
            done_count += 1
        else:
            c.execute(
                "UPDATE tasks SET status='failed', completed_at=?, "
                "error=?, method=? WHERE id=?",
                (now, r.get("error", "unknown"), r.get("method"), r["task_id"]),
            )
            fail_count += 1

    if worker_name:
        c.execute(
            "UPDATE workers SET tasks_completed = tasks_completed + ?, "
            "tasks_failed = tasks_failed + ?, last_seen = ? WHERE name = ?",
            (done_count, fail_count, now, worker_name),
        )
    c.commit()


def recover_stale(minutes: int = 10) -> int:
    """Reassign tasks that have been assigned for too long."""
    c = _conn()
    cutoff = time.time() - minutes * 60
    cursor = c.execute(
        "UPDATE tasks SET status='pending', worker=NULL, assigned_at=NULL "
        "WHERE status='assigned' AND assigned_at < ?",
        (cutoff,),
    )
    c.commit()
    return cursor.rowcount


def register_worker(name: str, ip: str, cores: int):
    c = _conn()
    c.execute(
        "INSERT INTO workers (name, ip, cores, last_seen) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET ip=?, cores=?, last_seen=?",
        (name, ip, cores, time.time(), ip, cores, time.time()),
    )
    c.commit()


def heartbeat_worker(name: str):
    c = _conn()
    c.execute("UPDATE workers SET last_seen=? WHERE name=?", (time.time(), name))
    c.commit()


def update_worker_stats(name: str, stats: dict):
    c = _conn()
    c.execute(
        "UPDATE workers SET cpu_pct=?, ram_used_gb=?, ram_total_gb=?, "
        "gpu_pct=?, gpu_temp=?, cpu_temp=?, last_seen=? WHERE name=?",
        (stats.get("cpu_pct"), stats.get("ram_used_gb"), stats.get("ram_total_gb"),
         stats.get("gpu_pct"), stats.get("gpu_temp"), stats.get("cpu_temp"),
         time.time(), name),
    )
    c.commit()


def get_workers() -> list[dict]:
    rows = _conn().execute("SELECT * FROM workers ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def get_stats() -> dict:
    c = _conn()
    counts = {}
    for row in c.execute(
        "SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status"
    ):
        counts[row["status"]] = row["cnt"]

    methods = {}
    for row in c.execute(
        "SELECT method, COUNT(*) as cnt FROM tasks WHERE status='done' GROUP BY method"
    ):
        methods[row["method"] or "unknown"] = row["cnt"]

    total = sum(counts.values())
    return {
        "total": total,
        "pending": counts.get("pending", 0),
        "assigned": counts.get("assigned", 0),
        "done": counts.get("done", 0),
        "failed": counts.get("failed", 0),
        "methods": methods,
    }


def log_rate():
    """Log current completed count for rate tracking."""
    c = _conn()
    done = c.execute("SELECT COUNT(*) FROM tasks WHERE status='done'").fetchone()[0]
    c.execute(
        "INSERT OR REPLACE INTO rate_log (timestamp, completed_count) VALUES (?, ?)",
        (time.time(), done),
    )
    # Keep only last 30 minutes (180 entries at 10s intervals)
    cutoff = time.time() - 1800
    c.execute("DELETE FROM rate_log WHERE timestamp < ?", (cutoff,))
    c.commit()


def get_rate_info() -> dict:
    """Calculate current rate and history from rate_log."""
    c = _conn()
    rows = c.execute(
        "SELECT timestamp, completed_count FROM rate_log ORDER BY timestamp"
    ).fetchall()

    if len(rows) < 2:
        return {"rate_per_sec": 0, "eta_seconds": 0, "history": []}

    # Current rate: average over last 60 seconds
    now = time.time()
    recent = [r for r in rows if r["timestamp"] > now - 60]
    if len(recent) >= 2:
        delta_tasks = recent[-1]["completed_count"] - recent[0]["completed_count"]
        delta_time = recent[-1]["timestamp"] - recent[0]["timestamp"]
        rate = delta_tasks / delta_time if delta_time > 0 else 0
    else:
        delta_tasks = rows[-1]["completed_count"] - rows[-2]["completed_count"]
        delta_time = rows[-1]["timestamp"] - rows[-2]["timestamp"]
        rate = delta_tasks / delta_time if delta_time > 0 else 0

    # ETA
    stats = get_stats()
    remaining = stats["pending"] + stats["assigned"]
    eta = remaining / rate if rate > 0 else 0

    # History: rate at each logged point
    history = []
    for i in range(1, len(rows)):
        dt = rows[i]["timestamp"] - rows[i - 1]["timestamp"]
        dc = rows[i]["completed_count"] - rows[i - 1]["completed_count"]
        history.append(round(dc / dt, 1) if dt > 0 else 0)

    return {
        "rate_per_sec": round(rate, 1),
        "eta_seconds": int(eta),
        "history": history,
    }


def get_task_path(task_id: int) -> str | None:
    row = _conn().execute(
        "SELECT pdf_path FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    return row["pdf_path"] if row else None


def get_task_text_path(task_id: int) -> str | None:
    row = _conn().execute(
        "SELECT text_path FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    return row["text_path"] if row else None

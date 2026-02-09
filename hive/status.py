import sys
import time

import requests


def print_status(coordinator: str, watch: bool = False):
    if not coordinator.startswith("http"):
        coordinator = f"http://{coordinator}"
    url = coordinator.rstrip("/")

    while True:
        try:
            resp = requests.get(f"{url}/tasks/stats", timeout=10)
            data = resp.json()
        except Exception as e:
            print(f"Error connecting to coordinator: {e}")
            if not watch:
                sys.exit(1)
            time.sleep(5)
            continue

        if watch:
            print("\033[2J\033[H", end="")  # Clear screen

        total = data.get("total", 0)
        done = data.get("done", 0)
        pending = data.get("pending", 0)
        assigned = data.get("assigned", 0)
        failed = data.get("failed", 0)
        rate = data.get("rate_per_sec", 0)
        eta = data.get("eta_seconds", 0)

        pct = (done / total * 100) if total > 0 else 0
        bar_width = 40
        filled = int(bar_width * pct / 100)
        bar = "#" * filled + "-" * (bar_width - filled)

        eta_str = _fmt_eta(eta)

        print("HIVE - Cluster Status")
        print("=" * 60)
        print(f"  Total: {total:>10,}    Done: {done:>10,}")
        print(f"  Speed: {rate:>9.1f}/s    ETA:  {eta_str:>10}")
        print(f"\n  [{bar}] {pct:.1f}%")
        print(f"\n  pending: {pending:,}  assigned: {assigned:,}  failed: {failed:,}")

        # Methods
        methods = data.get("methods", {})
        if methods:
            parts = [f"{k}: {v:,}" for k, v in methods.items()]
            print(f"  methods: {', '.join(parts)}")

        # Workers
        workers = data.get("workers", [])
        if workers:
            print(f"\n  Workers ({len(workers)}):")
            print(f"  {'Name':<15} {'Cores':>5} {'Done':>10} {'Failed':>8} {'Status':<10}")
            now = time.time()
            for w in workers:
                last = w.get("last_seen", 0)
                age = now - last if last else 999
                status = "working" if age < 60 else "stale"
                print(f"  {w['name']:<15} {w['cores']:>5} {w['tasks_completed']:>10,} {w['tasks_failed']:>8} {status:<10}")

        print()

        if not watch:
            break
        time.sleep(5)


def _fmt_eta(seconds: int) -> str:
    if seconds <= 0:
        return "-"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"

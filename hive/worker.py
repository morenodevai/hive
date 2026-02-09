import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import psutil
import requests

from hive.extract import extract_text

_shutdown = False


def _collect_system_stats() -> dict:
    """Collect CPU, RAM, GPU, and temperature stats."""
    stats = {
        "cpu_pct": psutil.cpu_percent(interval=None),
        "ram_used_gb": round((psutil.virtual_memory().used) / (1024**3), 1),
        "ram_total_gb": round((psutil.virtual_memory().total) / (1024**3), 1),
        "gpu_pct": None,
        "gpu_temp": None,
        "cpu_temp": None,
    }

    # CPU temperature - platform specific
    try:
        temps = psutil.sensors_temperatures() if hasattr(psutil, "sensors_temperatures") else {}
        if temps:
            # Linux: k10temp (AMD) - prefer Tctl (core temp) over Tccd
            if "k10temp" in temps:
                for entry in temps["k10temp"]:
                    if "tctl" in entry.label.lower():
                        stats["cpu_temp"] = entry.current
                        break
                # Fallback to first k10temp entry if Tctl not found
                if stats["cpu_temp"] is None and temps["k10temp"]:
                    stats["cpu_temp"] = temps["k10temp"][0].current
            # Intel: coretemp
            elif "coretemp" in temps and temps["coretemp"]:
                stats["cpu_temp"] = temps["coretemp"][0].current
            # Other thermal sensors
            else:
                for chip in ("cpu_thermal", "cpu-thermal", "acpitz"):
                    if chip in temps and temps[chip]:
                        stats["cpu_temp"] = temps[chip][0].current
                        break
    except Exception:
        pass

    # Mac: try /usr/bin/sysctl (if available)
    if stats["cpu_temp"] is None and sys.platform == "darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.temperature"],
                capture_output=True, text=True, timeout=2,
            )
            if out.returncode == 0:
                stats["cpu_temp"] = round(float(out.stdout.strip()), 1)
        except Exception:
            pass

    # Windows: try WMI via wmic (if available)
    if stats["cpu_temp"] is None and sys.platform == "win32":
        try:
            out = subprocess.run(
                ["wmic", "os", "get", "CurrentTimeZone"],
                capture_output=True, text=True, timeout=2,
            )
            # If wmic works, try getting CPU temp (varies by system)
            if out.returncode == 0:
                out = subprocess.run(
                    ["wmic", "path", "win32_temperaturemonitor", "get", "CurrentReading"],
                    capture_output=True, text=True, timeout=2,
                )
                if out.returncode == 0 and "CurrentReading" in out.stdout:
                    try:
                        lines = [l.strip() for l in out.stdout.split('\n') if l.strip() and l.strip() != 'CurrentReading']
                        if lines:
                            stats["cpu_temp"] = round(int(lines[0]) / 10, 1)  # WMI returns in 0.1K units
                    except Exception:
                        pass
        except Exception:
            pass

    # GPU stats via nvidia-smi (works on Linux with Nvidia GPUs)
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,temperature.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0:
            parts = out.stdout.strip().split(",")
            if len(parts) >= 2:
                try:
                    stats["gpu_pct"] = float(parts[0].strip())
                    stats["gpu_temp"] = float(parts[1].strip())
                except ValueError:
                    pass
    except (FileNotFoundError, Exception):
        pass

    return stats


def _signal_handler(sig, frame):
    global _shutdown
    print("\n[worker] Shutting down after current batch...")
    _shutdown = True


def _process_task(task: dict, local_pdf_dir: str | None, local_text_dir: str | None,
                  coordinator_url: str) -> dict:
    """Process a single task. Runs in a subprocess via ProcessPoolExecutor."""
    task_id = task["task_id"]
    pdf_path = task["pdf_path"]

    try:
        # Determine where to read the PDF from
        if local_pdf_dir:
            local_pdf = pdf_path  # Already a full local path
        else:
            # Download from coordinator
            resp = requests.get(f"{coordinator_url}/files/{task_id}", timeout=120)
            if resp.status_code != 200:
                return {"task_id": task_id, "status": "failed",
                        "error": f"Download failed: HTTP {resp.status_code}"}
            tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
            tmp.write(resp.content)
            tmp.close()
            local_pdf = tmp.name

        # Determine output path
        if local_text_dir:
            # Mirror the directory structure
            # pdf_path is the full source path, we need the relative part
            # We'll derive it from the filename
            basename = os.path.splitext(os.path.basename(pdf_path))[0] + ".txt"
            # Try to preserve subdirectory structure
            parts = pdf_path.replace("\\", "/").split("/")
            # Find 'pdfs' in path and take everything after it
            try:
                idx = parts.index("pdfs")
                rel = "/".join(parts[idx + 1:])
            except ValueError:
                rel = basename
            text_out = os.path.join(local_text_dir, os.path.splitext(rel)[0] + ".txt")
        else:
            text_out = tempfile.NamedTemporaryFile(suffix=".txt", delete=False).name

        # Extract
        result = extract_text(local_pdf, text_out)
        result["task_id"] = task_id

        # Upload text if not writing locally
        if not local_text_dir and result["status"] == "done" and os.path.exists(text_out):
            with open(text_out, "rb") as f:
                requests.post(f"{coordinator_url}/files/upload/{task_id}",
                              data=f.read(), timeout=120)
            os.unlink(text_out)

        # Clean up downloaded PDF
        if not local_pdf_dir and os.path.exists(local_pdf):
            os.unlink(local_pdf)

        return result

    except Exception as e:
        return {"task_id": task_id, "status": "failed",
                "error": str(e)[:200], "method": None, "char_count": 0}


def run_worker(coordinator: str, cpus: int, batch_size: int,
               local_pdf_dir: str | None, local_text_dir: str | None,
               name: str | None):
    global _shutdown

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    if not coordinator.startswith("http"):
        coordinator = f"http://{coordinator}"
    coordinator_url = coordinator.rstrip("/")

    if cpus <= 0:
        cpus = os.cpu_count() or 4

    if name is None:
        name = platform.node().split(".")[0].lower() or "worker"

    print(f"[worker:{name}] Connecting to {coordinator_url}")
    print(f"[worker:{name}] CPUs: {cpus}, batch: {batch_size}")
    if local_pdf_dir:
        print(f"[worker:{name}] Local PDF dir: {local_pdf_dir}")
    if local_text_dir:
        print(f"[worker:{name}] Local text dir: {local_text_dir}")

    # Register with coordinator
    while not _shutdown:
        try:
            resp = requests.post(f"{coordinator_url}/workers/register",
                                 json={"name": name, "cores": cpus}, timeout=10)
            if resp.status_code == 200:
                print(f"[worker:{name}] Registered with coordinator")
                break
        except requests.ConnectionError:
            print(f"[worker:{name}] Coordinator not reachable, retrying in 5s...")
            time.sleep(5)

    # Main loop
    consecutive_empty = 0
    while not _shutdown:
        try:
            # Pull tasks
            resp = requests.post(
                f"{coordinator_url}/tasks/pull",
                json={"worker": name, "batch_size": batch_size},
                timeout=30,
            )
            tasks = resp.json()

            if not tasks:
                consecutive_empty += 1
                if consecutive_empty == 1:
                    print(f"[worker:{name}] No tasks available, waiting...")
                time.sleep(5)
                continue

            consecutive_empty = 0
            print(f"[worker:{name}] Got {len(tasks)} tasks")

            # Process batch in parallel
            results = []
            with ProcessPoolExecutor(max_workers=cpus) as executor:
                futures = {
                    executor.submit(
                        _process_task, task, local_pdf_dir, local_text_dir, coordinator_url
                    ): task
                    for task in tasks
                }
                for future in as_completed(futures):
                    if _shutdown:
                        break
                    try:
                        result = future.result(timeout=90)
                        results.append(result)
                    except Exception as e:
                        task = futures[future]
                        results.append({
                            "task_id": task["task_id"],
                            "status": "failed",
                            "error": str(e)[:200],
                            "method": None,
                            "char_count": 0,
                        })

            # Report results + system stats
            if results:
                for r in results:
                    r["worker"] = name
                try:
                    requests.post(
                        f"{coordinator_url}/tasks/report",
                        json={"results": results},
                        timeout=30,
                    )
                    done = sum(1 for r in results if r["status"] == "done")
                    failed = sum(1 for r in results if r["status"] == "failed")
                    print(f"[worker:{name}] Reported: {done} done, {failed} failed")
                except Exception as e:
                    print(f"[worker:{name}] Failed to report results: {e}")

                # Send system stats
                try:
                    stats = _collect_system_stats()
                    requests.post(
                        f"{coordinator_url}/workers/stats",
                        json={"name": name, "stats": stats},
                        timeout=10,
                    )
                except Exception:
                    pass

        except requests.ConnectionError:
            print(f"[worker:{name}] Lost connection, retrying in 10s...")
            time.sleep(10)
        except Exception as e:
            print(f"[worker:{name}] Error: {e}")
            time.sleep(5)

    print(f"[worker:{name}] Shutdown complete")

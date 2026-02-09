# Hive - Learnings & Recommendations

## Project Summary

Hive is a distributed PDF text extraction tool. A coordinator server queues PDF files, workers across the network pull batches, extract text using a 4-tier fallback (pdftotext → PyMuPDF → PaddleOCR → Tesseract), and report results back. It includes a live web dashboard for monitoring.

**First production run:** 512,521 PDFs processed to completion across a 3-machine cluster (Linux 16-core, Mac 10-core, Windows OptiPlex 12-core).

---

## What Worked Well

- **4-tier extraction fallback** — pdftotext handled 92% of files, PyMuPDF caught another 3%, OCR covered scanned docs, and empty PDFs were gracefully marked done (not failed)
- **Batch processing with ProcessPoolExecutor** — efficient parallel extraction across all cores
- **Stale task recovery** — the coordinator's background thread automatically reclaims tasks from dead workers after N minutes
- **SQLite with WAL mode** — handled concurrent reads/writes from multiple workers without issues
- **Live dashboard** — real-time visibility into cluster health, progress, and worker stats

## What Went Wrong

### 1. Function Signature Mismatch (12,300 failures)
**Problem:** Adding `--pdf-ssh` and `--text-ssh` changed `_process_task()` signature. Workers running old code called it with the wrong number of arguments.
**Root cause:** No versioning or compatibility check between coordinator and workers.
**Fix applied:** Restarted workers with updated code, reset failed tasks to pending.

### 2. SCP Failures on Mac/OptiPlex (513 failures)
**Problem:** Mac and OptiPlex workers used `--pdf-ssh` to download PDFs via SCP. Some files failed to download (likely transient network/concurrency issues).
**Fix applied:** Re-ran failures on the Linux worker which had `--local-pdf-dir` (direct disk access, no SCP needed). All 513 completed successfully.

### 3. Orphaned Subprocess Workers
**Problem:** When the SSH session to the OptiPlex died, the main worker loop died but `ProcessPoolExecutor` subprocesses kept running — burning 99% CPU doing nothing useful.
**Root cause:** SSH session termination kills the parent process; child processes become orphans.
**Fix applied:** Killed orphaned processes, restarted via PowerShell `Start-Process` with `-WindowStyle Hidden` for detachment.

### 4. Windows Worker Persistence
**Problem:** Starting the OptiPlex worker via SSH was fragile. `start /b`, `nohup`, and PowerShell `Start-Process` all had reliability issues — processes would sometimes not start or die immediately.
**Root cause:** Windows SSH service sessions don't behave like interactive sessions for background process management.

### 5. Dashboard Stale Data
**Problem:** When a worker went stale, the dashboard still showed its last-reported CPU/RAM stats (e.g., 99% CPU on a machine that was off).
**Root cause:** Worker stats in the DB are only updated on heartbeat; no mechanism to clear them on timeout.

---

## Recommended Improvements

### High Priority

1. **Worker version handshake** — On registration, workers should send their code version. Coordinator rejects workers running incompatible versions. Prevents silent failures from signature mismatches.

2. **Worker auto-update** — `git pull && pip install .` on worker startup (Linux already does this via the bash launch script). Make it standard for all platforms.

3. **Windows service wrapper** — Use `pythonw.exe` or NSSM (Non-Sucking Service Manager) to run the worker as a proper Windows service instead of relying on SSH session persistence.

4. **Dashboard: clear stale worker stats** — When a worker hasn't heartbeated for >60s, zero out or grey out its CPU/RAM/temp stats instead of showing stale data.

5. **Prefer local file access** — When a worker has direct access to the PDF/text filesystem (NFS mount, local disk), always prefer that over SCP. The SCP path should be a fallback, not a default.

### Medium Priority

6. **Retry logic with backoff** — Failed tasks should auto-retry 2-3 times with exponential backoff before being marked as permanently failed. Currently they fail once and stay failed until manually reset.

7. **Task assignment affinity** — Route tasks to workers best suited for them. Workers with `--local-pdf-dir` should get priority over SCP-based workers to avoid unnecessary network transfers.

8. **Heartbeat in subprocess pool** — Send heartbeats during long-running batches (e.g., OCR on 100 PDFs), not just between batches. Prevents the coordinator from marking an active worker as stale during a slow batch.

9. **Graceful shutdown for subprocesses** — On SIGTERM/SIGINT, wait for in-flight subprocesses to complete and report results before exiting. Currently, in-flight work is lost and must be recovered via stale recovery.

10. **Structured logging with timestamps** — Add timestamps to log output (`[2026-02-08 21:35:23]`). Currently impossible to tell if a log entry is recent or hours old.

### Low Priority

11. **Progress within batch** — Dashboard could show "Worker X: 47/100 in current batch" instead of just assigned count.

12. **Failed task inspector** — Dashboard page listing all failed tasks with their error messages, with a "Retry All" button.

13. **Worker health alerts** — Optional Discord/webhook notification when a worker goes stale or CPU temp exceeds threshold.

14. **NFS mount option** — For cross-machine file access, NFS mount would be faster and more reliable than SCP for every file.

---

## Cluster Setup Notes

### Linux (main workhorse)
- 16 cores, local PDF/text dirs on attached storage
- Launched via: `cd /tmp/hive && git pull && pip install --user . && nohup hive worker ... &`
- Handles OCR workloads best (fastest CPU)

### Mac (coordinator + worker)
- 10 cores, runs coordinator on port 9000
- Worker uses `--pdf-ssh` / `--text-ssh` for file access
- Has all 4 extraction tools: pdftotext, PyMuPDF, PaddleOCR, Tesseract
- Coordinator command: `hive coordinator --port 9000 --pdf-source ssh://lalo@192.168.4.68:/path --text-dest ssh://lalo@192.168.4.68:/path`

### OptiPlex (Windows)
- 12 cores, Python 3.14 at `C:\Python314\`
- SSH via `OPTIPLEX-JELLY@192.168.4.220`
- Worker uses `--pdf-ssh` / `--text-ssh`
- Start detached: `powershell Start-Process -FilePath 'C:\Python314\python.exe' -ArgumentList '-u','-m','hive','worker',... -WindowStyle Hidden`
- Fragile — consider NSSM or Windows Task Scheduler for reliability

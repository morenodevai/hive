import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="hive",
        description="Distributed task processing tool",
    )
    sub = parser.add_subparsers(dest="command")

    # --- coordinator ---
    coord = sub.add_parser("coordinator", help="Start the coordinator server")
    coord.add_argument("--port", type=int, default=9000)
    coord.add_argument(
        "--pdf-source",
        required=True,
        help="Local path or ssh://user@host:/path to PDF directory",
    )
    coord.add_argument(
        "--text-dest",
        required=True,
        help="Local path or ssh://user@host:/path for text output",
    )
    coord.add_argument("--stale-minutes", type=int, default=10,
                        help="Reassign tasks older than N minutes (default 10)")

    # --- worker ---
    wrk = sub.add_parser("worker", help="Start a worker")
    wrk.add_argument("--coordinator", required=True, help="host:port of coordinator")
    wrk.add_argument("--cpus", type=int, default=0,
                      help="CPU cores to use (0 = all)")
    wrk.add_argument("--batch-size", type=int, default=50)
    wrk.add_argument("--local-pdf-dir",
                      help="Local path to PDFs (skip download)")
    wrk.add_argument("--local-text-dir",
                      help="Local path to write text output")
    wrk.add_argument("--name", help="Worker name (default: hostname)")

    # --- status ---
    st = sub.add_parser("status", help="Show cluster status")
    st.add_argument("--coordinator", required=True, help="host:port of coordinator")
    st.add_argument("--watch", action="store_true", help="Refresh every 5s")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "coordinator":
        from hive.coordinator import run_coordinator
        run_coordinator(
            port=args.port,
            pdf_source=args.pdf_source,
            text_dest=args.text_dest,
            stale_minutes=args.stale_minutes,
        )

    elif args.command == "worker":
        from hive.worker import run_worker
        run_worker(
            coordinator=args.coordinator,
            cpus=args.cpus,
            batch_size=args.batch_size,
            local_pdf_dir=args.local_pdf_dir,
            local_text_dir=args.local_text_dir,
            name=args.name,
        )

    elif args.command == "status":
        from hive.status import print_status
        print_status(args.coordinator, watch=args.watch)

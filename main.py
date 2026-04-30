#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Sequence

from obeta_etl.config import DBConfig
from obeta_etl.db import connect
from obeta_etl.diagnostics import print_counts
from obeta_etl.flow1_raw_to_staging import flow1_raw_to_staging
from obeta_etl.flow2_transform_staging import flow2_transform_staging
from obeta_etl.flow3_staging_to_production import flow3_staging_to_production
from obeta_etl.logger import log


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OBETA Tableau Prep flows rewritten in Python with batched DB writes"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--csv-chunk-size", type=int, default=50_000, help="Rows per pandas CSV chunk")
        p.add_argument("--db-batch-size", type=int, default=100_000, help="Rows/source IDs per DB batch commit")
        p.add_argument("--reset-output", action="store_true", help="Truncate/drop target tables before writing")
        p.add_argument("--product-encoding", default="cp1252", help="Encoding for 002 product_data.csv")

    p1 = sub.add_parser("flow1", help="Run Flow 1 only: raw CSV -> staging")
    add_common(p1)
    p1.add_argument("--warehouse-csv", required=True, type=Path)
    p1.add_argument("--product-csv", required=True, type=Path)
    p1.add_argument("--pick-csv", required=True, type=Path)

    p2 = sub.add_parser("flow2", help="Run Flow 2 only: staging -> pickoperations")
    add_common(p2)

    p3 = sub.add_parser("flow3", help="Run Flow 3 only: pickoperations -> production")
    add_common(p3)

    pall = sub.add_parser("run-all", help="Run Flow 1, Flow 2 and Flow 3")
    add_common(pall)
    pall.add_argument("--warehouse-csv", required=True, type=Path)
    pall.add_argument("--product-csv", required=True, type=Path)
    pall.add_argument("--pick-csv", required=True, type=Path)

    pc = sub.add_parser("counts", help="Print row counts for staging and production tables")
    add_common(pc)

    return parser


def validate_paths(args: argparse.Namespace) -> None:
    for attr in ["warehouse_csv", "product_csv", "pick_csv"]:
        path = getattr(args, attr, None)
        if path is not None and not path.exists():
            raise FileNotFoundError(f"File not found: {path}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_paths(args)

    cfg = DBConfig.from_env()
    started = time.time()

    log(f"Command selected: {args.command}")
    log(f"Database: {cfg.user}@{cfg.host}:{cfg.port}; staging={cfg.staging_db}; production={cfg.production_db}")

    conn = connect(cfg)
    try:
        if args.command == "flow1":
            flow1_raw_to_staging(
                conn,
                cfg,
                args.warehouse_csv,
                args.product_csv,
                args.pick_csv,
                args.csv_chunk_size,
                args.db_batch_size,
                args.reset_output,
                args.product_encoding,
            )
        elif args.command == "flow2":
            flow2_transform_staging(conn, cfg, args.db_batch_size, args.reset_output)
        elif args.command == "flow3":
            flow3_staging_to_production(conn, cfg, args.db_batch_size, args.reset_output)
        elif args.command == "run-all":
            flow1_raw_to_staging(
                conn,
                cfg,
                args.warehouse_csv,
                args.product_csv,
                args.pick_csv,
                args.csv_chunk_size,
                args.db_batch_size,
                args.reset_output,
                args.product_encoding,
            )
            flow2_transform_staging(conn, cfg, args.db_batch_size, args.reset_output)
            flow3_staging_to_production(conn, cfg, args.db_batch_size, args.reset_output)
        elif args.command == "counts":
            print_counts(conn, cfg)
        else:
            parser.error(f"Unknown command: {args.command}")

        print_counts(conn, cfg)
        elapsed = time.time() - started
        log(f"DONE in {elapsed:,.1f} seconds")
        return 0

    except Exception as exc:
        conn.rollback()
        log(f"ERROR: {type(exc).__name__}: {exc}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
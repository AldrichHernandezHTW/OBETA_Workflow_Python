from __future__ import annotations

from obeta_etl.config import DBConfig
from obeta_etl.db import fetch_one, qname
from obeta_etl.logger import log


def print_counts(conn, cfg: DBConfig) -> None:
    tables = [
        (cfg.staging_db, "warehouse_sections"),
        (cfg.staging_db, "product_data"),
        (cfg.staging_db, "pick_data"),
        (cfg.staging_db, "pickoperations"),
        (cfg.production_db, "product"),
        (cfg.production_db, "order"),
        (cfg.production_db, "pick"),
    ]

    log("Row counts")
    for db, table in tables:
        exists = fetch_one(conn, """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (db, table))[0]

        if not exists:
            print(f"  {db}.{table}: table does not exist")
            continue

        count = fetch_one(conn, f"SELECT COUNT(*) FROM {qname(db, table)}")[0]
        print(f"  {db}.{table}: {count:,}")
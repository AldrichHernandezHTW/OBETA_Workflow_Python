from __future__ import annotations

from pathlib import Path

import pandas as pd

from obeta_etl.config import DBConfig
from obeta_etl.dataframe_writer import batch_insert_dataframe
from obeta_etl.db import ensure_databases
from obeta_etl.ddl import create_staging_tables
from obeta_etl.indexes import add_index_if_missing
from obeta_etl.logger import log


def flow1_raw_to_staging(
    conn,
    cfg: DBConfig,
    warehouse_csv: Path,
    product_csv: Path,
    pick_csv: Path,
    csv_chunk_size: int,
    db_batch_size: int,
    reset_output: bool,
    product_encoding: str = "cp1252",
) -> None:
    log("START Flow 1: Raw CSV files -> OBETA_STAGING")
    ensure_databases(conn, cfg)
    create_staging_tables(conn, cfg, reset_output=reset_output)

    log("Loading warehouse_sections")
    wh_cols = ["warehouse_abbreviation", "warehouse_description", "warehouse_section"]
    wh_reader = pd.read_csv(
        warehouse_csv,
        sep=";",
        header=0,
        encoding="utf-8",
        chunksize=csv_chunk_size,
        dtype=str,
    )

    total = 0
    for chunk in wh_reader:
        chunk = chunk.rename(
            columns={
                "Abkürzung": "warehouse_abbreviation",
                "Bedeutung": "warehouse_description",
                "Gruppe": "warehouse_section",
            }
        )
        chunk = chunk[[c for c in wh_cols if c in chunk.columns]]
        missing = [c for c in wh_cols if c not in chunk.columns]
        if missing:
            raise ValueError(f"warehouse_sections CSV is missing columns after rename: {missing}")

        total += batch_insert_dataframe(conn, cfg.staging_db, "warehouse_sections", chunk[wh_cols], db_batch_size)

    log(f"warehouse_sections inserted rows: {total:,}")

    log("Loading product_data")
    product_cols = ["product_id", "product_description", "product_group"]
    product_reader = pd.read_csv(
        product_csv,
        sep=",",
        header=None,
        names=product_cols,
        encoding=product_encoding,
        chunksize=csv_chunk_size,
        dtype=str,
    )

    total = 0
    for chunk in product_reader:
        total += batch_insert_dataframe(conn, cfg.staging_db, "product_data", chunk[product_cols], db_batch_size)

    log(f"product_data inserted rows: {total:,}")

    log("Loading pick_data")
    pick_cols = [
        "product_id",
        "warehouse_section",
        "origin",
        "order_id",
        "position_in_order",
        "pick_volume",
        "quantity_unit",
        "date",
    ]

    pick_reader = pd.read_csv(
        pick_csv,
        sep=",",
        header=None,
        names=pick_cols,
        encoding="utf-8",
        chunksize=csv_chunk_size,
        dtype={
            "product_id": "string",
            "warehouse_section": "string",
            "quantity_unit": "string",
        },
    )

    total = 0
    for chunk in pick_reader:
        for c in ["origin", "order_id", "position_in_order", "pick_volume"]:
            chunk[c] = pd.to_numeric(chunk[c], errors="coerce")

        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")
        total += batch_insert_dataframe(conn, cfg.staging_db, "pick_data", chunk[pick_cols], db_batch_size)

    log(f"pick_data inserted rows: {total:,}")

    log("Creating staging indexes for Flow 2")
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_order_date", "order_id, `date`")
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_warehouse", "warehouse_section")
    add_index_if_missing(conn, cfg.staging_db, "product_data", "idx_product_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "warehouse_sections", "idx_warehouse_abbreviation", "warehouse_abbreviation")
    conn.commit()

    log("END Flow 1")
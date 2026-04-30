from __future__ import annotations

from obeta_etl.config import DBConfig
from obeta_etl.db import col_list, ensure_databases, execute, fetch_one, qname
from obeta_etl.ddl import create_pickoperations_table
from obeta_etl.indexes import add_index_if_missing
from obeta_etl.logger import log


def flow2_transform_staging(conn, cfg: DBConfig, db_batch_size: int, reset_output: bool) -> None:
    log("START Flow 2: Transform/Clean staging -> pickoperations")
    ensure_databases(conn, cfg)
    execute(conn, f"USE `{cfg.staging_db}`")
    create_pickoperations_table(conn, cfg, reset_output=reset_output)

    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "product_data", "idx_product_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "warehouse_sections", "idx_warehouse_abbreviation", "warehouse_abbreviation")
    conn.commit()

    log("Creating temporary duration table: MIN/MAX date by pick_id")
    execute(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_flow2_duration")
    execute(conn, """
        CREATE TEMPORARY TABLE tmp_flow2_duration (
            pick_id VARCHAR(50) NOT NULL,
            time_to_fill_minutes DECIMAL(12,6),
            INDEX idx_tmp_flow2_duration_pick_id (pick_id)
        ) ENGINE=InnoDB
    """)

    execute(conn, f"""
        INSERT INTO tmp_flow2_duration (pick_id, time_to_fill_minutes)
        SELECT
            x.pick_id,
            TIMESTAMPDIFF(MINUTE, MIN(x.`date`), MAX(x.`date`)) AS time_to_fill_minutes
        FROM (
            SELECT
                CONCAT(CAST(pd.order_id AS CHAR), CAST(YEAR(pd.`date`) AS CHAR)) AS pick_id,
                pd.`date`
            FROM {qname(cfg.staging_db, 'pick_data')} pd
        ) x
        WHERE x.pick_id IS NOT NULL
        GROUP BY x.pick_id
        HAVING time_to_fill_minutes <= 10080
    """)
    conn.commit()

    log("Creating temporary transformed pickoperations table")
    execute(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_flow2_pickoperations")
    execute(conn, """
        CREATE TEMPORARY TABLE tmp_flow2_pickoperations (
            etl_row_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            pick_id VARCHAR(50),
            order_id VARCHAR(50),
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100),
            warehouse_section VARCHAR(50),
            origin VARCHAR(50),
            position_in_order VARCHAR(20),
            pick_volume INT,
            quantity_unit VARCHAR(10),
            year INT,
            month INT,
            quarter INT,
            order_duration DECIMAL(12,6),
            INDEX idx_tmp_flow2_row_id (etl_row_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        INSERT INTO tmp_flow2_pickoperations (
            pick_id,
            order_id,
            product_id,
            product_description,
            product_group,
            warehouse_section,
            origin,
            position_in_order,
            pick_volume,
            quantity_unit,
            year,
            month,
            quarter,
            order_duration
        )
        SELECT
            p.pick_id,
            CAST(p.order_id AS CHAR) AS order_id,
            p.product_id,
            pr.product_description,
            pr.product_group,
            p.warehouse_section,
            CASE
                WHEN p.origin_num = 46 THEN 'STORE'
                WHEN p.origin_num = 48 THEN 'CUSTOMER'
                ELSE NULL
            END AS origin,
            CAST(p.position_in_order AS CHAR) AS position_in_order,
            p.pick_volume,
            p.quantity_unit,
            p.year,
            p.month,
            p.quarter,
            CAST(COALESCE(d.time_to_fill_minutes, 0) AS DECIMAL(12,6)) AS order_duration
        FROM (
            SELECT
                pd.product_id,
                pd.warehouse_section,
                pd.origin AS origin_num,
                pd.order_id,
                pd.position_in_order,
                pd.pick_volume,
                pd.quantity_unit,
                pd.`date`,
                YEAR(pd.`date`) AS year,
                MONTH(pd.`date`) AS month,
                QUARTER(pd.`date`) AS quarter,
                CONCAT(CAST(pd.order_id AS CHAR), CAST(YEAR(pd.`date`) AS CHAR)) AS pick_id
            FROM {qname(cfg.staging_db, 'pick_data')} pd
            WHERE pd.order_id IS NOT NULL
        ) p
        LEFT JOIN tmp_flow2_duration d
            ON d.pick_id COLLATE utf8mb4_unicode_ci = p.pick_id COLLATE utf8mb4_unicode_ci
        JOIN {qname(cfg.staging_db, 'product_data')} pr
            ON pr.product_id COLLATE utf8mb4_unicode_ci = p.product_id COLLATE utf8mb4_unicode_ci
        LEFT JOIN {qname(cfg.staging_db, 'warehouse_sections')} ws
            ON p.warehouse_section COLLATE utf8mb4_unicode_ci = ws.warehouse_abbreviation COLLATE utf8mb4_unicode_ci
        WHERE p.pick_volume > 0
          AND pr.product_description IS NOT NULL
          AND pr.product_group IS NOT NULL
    """)
    conn.commit()

    max_id = fetch_one(conn, "SELECT COALESCE(MAX(etl_row_id), 0) FROM tmp_flow2_pickoperations")[0]
    log(f"Writing pickoperations in batches. Rows to write: {max_id:,}")

    columns = [
        "pick_id",
        "order_id",
        "product_id",
        "product_description",
        "product_group",
        "warehouse_section",
        "origin",
        "position_in_order",
        "pick_volume",
        "quantity_unit",
        "year",
        "month",
        "quarter",
        "order_duration",
    ]

    for start_id in range(1, int(max_id) + 1, db_batch_size):
        end_id = start_id + db_batch_size - 1
        execute(conn, f"""
            INSERT INTO {qname(cfg.staging_db, 'pickoperations')} ({col_list(columns)})
            SELECT {col_list(columns)}
            FROM tmp_flow2_pickoperations
            WHERE etl_row_id BETWEEN %s AND %s
            ORDER BY etl_row_id
        """, (start_id, end_id))
        conn.commit()
        log(f"pickoperations batch committed: {start_id:,} - {min(end_id, int(max_id)):,}")

    log("Creating index on pickoperations for Flow 3")
    add_index_if_missing(conn, cfg.staging_db, "pickoperations", "idx_pickoperations_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "pickoperations", "idx_pickoperations_order", "order_id")
    conn.commit()

    log("END Flow 2")
from __future__ import annotations

from obeta_etl.config import DBConfig
from obeta_etl.db import ensure_databases, execute, fetch_one, qname
from obeta_etl.indexes import add_index_if_missing
from obeta_etl.logger import log


def flow3_staging_to_production(conn, cfg: DBConfig, db_batch_size: int, reset_output: bool) -> None:
    log("START Flow 3 FAST SQL: pickoperations -> production physical tables")
    ensure_databases(conn, cfg)
    execute(conn, f"USE `{cfg.staging_db}`")

    execute(conn, "SET SESSION foreign_key_checks = 0")
    execute(conn, "SET SESSION unique_checks = 0")

    if reset_output:
        log("Dropping existing production tables")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'pick')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product_lookup')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order_lookup')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order')}")

    log("Creating production product dimension")
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'product')} (
            product_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100),
            warehouse_section VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    log("Creating production order dimension")
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'order')} (
            order_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            order_id VARCHAR(50),
            origin VARCHAR(50),
            position_in_order VARCHAR(20)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    log("Creating production pick fact table")
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'pick')} (
            pick_id VARCHAR(50),
            product_key INT,
            order_key INT,
            pick_volume INT,
            quantity_unit VARCHAR(10),
            year INT,
            month INT,
            quarter INT,
            order_duration DECIMAL(12,6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    if reset_output:
        log("Truncating production tables")
        execute(conn, f"TRUNCATE TABLE {qname(cfg.production_db, 'pick')}")
        execute(conn, f"TRUNCATE TABLE {qname(cfg.production_db, 'product')}")
        execute(conn, f"TRUNCATE TABLE {qname(cfg.production_db, 'order')}")

    log("Loading product dimension directly from staging")
    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'product')} (
            product_id,
            product_description,
            product_group,
            warehouse_section
        )
        SELECT DISTINCT
            product_id,
            product_description,
            product_group,
            warehouse_section
        FROM {qname(cfg.staging_db, 'pickoperations')}
    """)
    conn.commit()

    log("Loading order dimension directly from staging")
    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'order')} (
            order_id,
            origin,
            position_in_order
        )
        SELECT DISTINCT
            order_id,
            origin,
            position_in_order
        FROM {qname(cfg.staging_db, 'pickoperations')}
    """)
    conn.commit()

    log("Creating indexes for dimension lookup")
    add_index_if_missing(conn, cfg.production_db, "product", "idx_product_product_id", "product_id")
    add_index_if_missing(conn, cfg.production_db, "order", "idx_order_order_id", "order_id")
    conn.commit()

    log("Creating physical product lookup table")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product_lookup')}")
    execute(conn, f"""
        CREATE TABLE {qname(cfg.production_db, 'product_lookup')} (
            product_id VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL,
            product_key INT NOT NULL,
            PRIMARY KEY(product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'product_lookup')} (
            product_id,
            product_key
        )
        SELECT
            product_id COLLATE utf8mb4_unicode_ci AS product_id,
            MIN(product_key) AS product_key
        FROM {qname(cfg.production_db, 'product')}
        GROUP BY product_id COLLATE utf8mb4_unicode_ci
    """)
    conn.commit()

    log("Creating physical order lookup table")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order_lookup')}")
    execute(conn, f"""
        CREATE TABLE {qname(cfg.production_db, 'order_lookup')} (
            order_id VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL,
            order_key INT NOT NULL,
            PRIMARY KEY(order_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'order_lookup')} (
            order_id,
            order_key
        )
        SELECT
            order_id COLLATE utf8mb4_unicode_ci AS order_id,
            MIN(order_key) AS order_key
        FROM {qname(cfg.production_db, 'order')}
        GROUP BY order_id COLLATE utf8mb4_unicode_ci
    """)
    conn.commit()

    log("Loading pick fact table with one internal SQL operation")
    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'pick')} (
            pick_id,
            product_key,
            order_key,
            pick_volume,
            quantity_unit,
            year,
            month,
            quarter,
            order_duration
        )
        SELECT
            s.pick_id,
            p.product_key,
            o.order_key,
            s.pick_volume,
            s.quantity_unit,
            s.year,
            s.month,
            s.quarter,
            s.order_duration
        FROM {qname(cfg.staging_db, 'pickoperations')} s
        JOIN {qname(cfg.production_db, 'product_lookup')} p
            ON s.product_id COLLATE utf8mb4_unicode_ci = p.product_id COLLATE utf8mb4_unicode_ci
        JOIN {qname(cfg.production_db, 'order_lookup')} o
            ON s.order_id COLLATE utf8mb4_unicode_ci = o.order_id COLLATE utf8mb4_unicode_ci
    """)
    conn.commit()

    log("Creating fact table indexes after load")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_product_key", "product_key")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_order_key", "order_key")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_year_month", "year, month")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_quarter", "quarter")
    conn.commit()

    log("Dropping helper lookup tables after fact load")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product_lookup')}")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order_lookup')}")
    conn.commit()

    log("Adding physical foreign key constraints at the end")

    execute(conn, "SET SESSION unique_checks = 1")
    execute(conn, "SET SESSION foreign_key_checks = 1")
    conn.commit()

    fk_product_exists = fetch_one(conn, """
        SELECT COUNT(*)
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE table_schema = %s
          AND table_name = 'pick'
          AND constraint_name = 'fk_pick_product'
          AND referenced_table_name = 'product'
    """, (cfg.production_db,))[0]

    if not fk_product_exists:
        execute(conn, f"""
            ALTER TABLE {qname(cfg.production_db, 'pick')}
            ADD CONSTRAINT fk_pick_product
            FOREIGN KEY (product_key)
            REFERENCES {qname(cfg.production_db, 'product')}(product_key)
        """)
        conn.commit()
        log("Foreign key created: pick.product_key -> product.product_key")
    else:
        log("Foreign key already exists: fk_pick_product")

    fk_order_exists = fetch_one(conn, """
        SELECT COUNT(*)
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE table_schema = %s
          AND table_name = 'pick'
          AND constraint_name = 'fk_pick_order'
          AND referenced_table_name = 'order'
    """, (cfg.production_db,))[0]

    if not fk_order_exists:
        execute(conn, f"""
            ALTER TABLE {qname(cfg.production_db, 'pick')}
            ADD CONSTRAINT fk_pick_order
            FOREIGN KEY (order_key)
            REFERENCES {qname(cfg.production_db, 'order')}(order_key)
        """)
        conn.commit()
        log("Foreign key created: pick.order_key -> order.order_key")
    else:
        log("Foreign key already exists: fk_pick_order")

    log("END Flow 3 FAST SQL")
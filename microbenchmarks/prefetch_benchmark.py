#!/usr/bin/env python3
"""
Prefetch benchmark script for PostgreSQL index scan prefetching.

Compares query performance across three configurations:
1. master (no prefetching, GUC not available)
2. patch with enable_indexscan_prefetch=off
3. patch with enable_indexscan_prefetch=on

Usage:
    ./prefetch_benchmark.py                    # Run all queries, uncached, 3 runs
    ./prefetch_benchmark.py --cached           # Run with data prewarmed
    ./prefetch_benchmark.py --queries Q1,Q2    # Run specific queries
    ./prefetch_benchmark.py --runs 5           # 5 runs per query
    ./prefetch_benchmark.py --skip-load        # Skip data loading verification
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import OrderedDict
from datetime import datetime
from statistics import mean

import psycopg

# --- Configuration ---

MASTER_BIN = "/mnt/nvme/postgresql/master/install_meson_rc/bin"
PATCH_BIN = "/mnt/nvme/postgresql/patch/install_meson_rc/bin"
MASTER_DATA_DIR = "/mnt/nvme/postgresql/master/data"
PATCH_DATA_DIR = "/mnt/nvme/postgresql/patch/data"
MASTER_SOURCE_DIR = "/mnt/nvme/postgresql/master/source"
PATCH_SOURCE_DIR = "/mnt/nvme/postgresql/patch/source"

MASTER_CONN = {
    "dbname": "regression",
    "user": "pg",
    "host": "/tmp",
    "port": 5555,
}
PATCH_CONN = {
    "dbname": "regression",
    "user": "pg",
    "host": "/tmp",
    "port": 5432,
}

# Expected row counts for data verification
EXPECTED_ORDERS = 50_000_000
EXPECTED_CUSTOMERS = 100_000
EXPECTED_PRODUCTS = 10_000
ROW_COUNT_TOLERANCE = 0.05  # 5%

# Output directory for results
OUTPUT_DIR = "prefetch_results"

# CPU pinning settings
BENCHMARK_CPU = 14

# --- Query Definitions ---

QUERIES = OrderedDict([
    ("Q1", {
        "name": "Simple date range scan",
        "sql": """
            SELECT order_id, customer_id, amount
            FROM prefetch_orders
            WHERE order_date BETWEEN '2023-06-01' AND '2023-06-15'
            ORDER BY order_date
            LIMIT 50000
        """,
        "evict": ["prefetch_orders"],
        "prewarm_indexes": ["prefetch_orders_date_idx"],
        "prewarm_tables": ["prefetch_orders"],
    }),
    ("Q2", {
        "name": "JOIN Orders + Customers",
        "sql": """
            SELECT o.order_id, c.customer_name, o.amount, o.order_date
            FROM prefetch_orders o
            JOIN prefetch_customers c ON c.customer_id = o.customer_id
            WHERE o.customer_id BETWEEN 5000 AND 5500
              AND o.order_date BETWEEN '2023-06-01' AND '2023-06-30'
            ORDER BY o.order_date
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
    }),
    # ("Q3", {
    #     "name": "Multi-JOIN Orders+Customers+Products",
    #     "sql": """
    #         SELECT o.order_id, c.customer_name, p.product_name, o.amount
    #         FROM prefetch_orders o
    #         JOIN prefetch_customers c ON c.customer_id = o.customer_id
    #         JOIN prefetch_products p ON p.product_id = o.product_id
    #         WHERE o.order_date BETWEEN '2023-03-01' AND '2023-03-15'
    #         ORDER BY o.order_date, o.order_id
    #         LIMIT 100000
    #     """,
    #     "evict": ["prefetch_orders", "prefetch_customers", "prefetch_products"],
    #     "prewarm_indexes": ["prefetch_orders_date_idx", "prefetch_customers_pkey", "prefetch_products_pkey"],
    #     "prewarm_tables": ["prefetch_orders", "prefetch_customers", "prefetch_products"],
    # }),
    ("Q4", {
        "name": "Aggregation with IN list",
        "sql": """
            SELECT order_date, sum(amount) AS total, count(*) AS orders
            FROM prefetch_orders
            WHERE customer_id IN (1000, 2000, 3000, 4000, 5000,
                                  6000, 7000, 8000, 9000, 10000)
              AND order_date BETWEEN '2023-01-01' AND '2023-12-31'
            GROUP BY order_date
            ORDER BY order_date
        """,
        "evict": ["prefetch_orders"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx"],
        "prewarm_tables": ["prefetch_orders"],
    }),
    ("Q5", {
        "name": "Backwards scan with JOIN",
        "sql": """
            SELECT o.order_id, c.customer_name, o.amount, o.order_date
            FROM prefetch_orders o
            JOIN prefetch_customers c ON c.customer_id = o.customer_id
            WHERE o.customer_id BETWEEN 10000 AND 10500
              AND o.order_date BETWEEN '2023-09-01' AND '2023-09-30'
            ORDER BY o.customer_id DESC, o.order_date DESC
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
        "gucs": {"enable_sort": "off"},
    }),
    ("Q6", {
        "name": "Nested Loop with correlated subquery",
        "sql": """
            SELECT c.customer_name, (
              SELECT sum(amount) FROM prefetch_orders o
              WHERE o.customer_id = c.customer_id
            )
            FROM prefetch_customers c
            WHERE c.customer_id BETWEEN 1000 AND 1100
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
    }),
    ("Q7", {
        "name": "LIMIT with large OFFSET",
        "sql": """
            SELECT * FROM prefetch_orders
            WHERE customer_id = 5000
            ORDER BY order_date
            OFFSET 400 LIMIT 100
        """,
        "evict": ["prefetch_orders"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx"],
        "prewarm_tables": ["prefetch_orders"],
    }),
    ("Q8", {
        "name": "Semi-join with EXISTS",
        "sql": """
            SELECT c.customer_id, c.customer_name
            FROM prefetch_customers c
            WHERE EXISTS (
              SELECT 1 FROM prefetch_orders o
              WHERE o.customer_id = c.customer_id
                AND o.order_date BETWEEN '2023-07-01' AND '2023-07-31'
                AND o.amount > 900
            )
            AND c.region_id = 5
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
    }),
    ("Q9", {
        "name": "Lateral join (Top N per group)",
        "sql": """
            SELECT c.customer_name, recent.*
            FROM prefetch_customers c,
            LATERAL (
              SELECT order_id, order_date, amount
              FROM prefetch_orders o
              WHERE o.customer_id = c.customer_id
              ORDER BY o.order_date DESC
              LIMIT 5
            ) recent
            WHERE c.customer_id BETWEEN 2000 AND 2100
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
    }),
    # ("Q10", {
    #     "name": "Range scan + selective filter",
    #     "sql": """
    #         SELECT order_id, customer_id, amount
    #         FROM prefetch_orders
    #         WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
    #           AND amount BETWEEN 999.00 AND 999.99
    #     """,
    #     "evict": ["prefetch_orders"],
    #     "prewarm_indexes": ["prefetch_orders_date_idx"],
    #     "prewarm_tables": ["prefetch_orders"],
    # }),
    # Adversarial queries
    ("A1", {
        "name": "Sequential heap access (adversarial)",
        "sql": """
            SELECT * FROM prefetch_sequential
            WHERE id BETWEEN 1 AND 200000
            ORDER BY id
        """,
        "evict": ["prefetch_sequential"],
        "prewarm_indexes": ["prefetch_sequential_idx"],
        "prewarm_tables": ["prefetch_sequential"],
        "setup_table": "prefetch_sequential",
    }),
    ("A2", {
        "name": "Tiny result set (adversarial)",
        "sql": """
            SELECT * FROM prefetch_orders
            WHERE customer_id = 50000
              AND order_date BETWEEN '2023-06-15' AND '2023-06-17'
        """,
        "evict": ["prefetch_orders"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx"],
        "prewarm_tables": ["prefetch_orders"],
    }),
    ("A3", {
        "name": "High cache hit ratio (adversarial)",
        "sql": """
            SELECT * FROM prefetch_orders
            WHERE customer_id = 1
              AND order_date BETWEEN '2023-01-01' AND '2023-12-31'
        """,
        # Special handling: no eviction, run query twice to warm cache first
        "evict": [],
        "prewarm_indexes": [],
        "prewarm_tables": [],
        "warmup_query": True,
    }),
    ("A4", {
        "name": "Early LIMIT termination (adversarial)",
        "sql": """
            SELECT * FROM prefetch_orders
            WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
            ORDER BY order_date
            LIMIT 10
        """,
        "evict": ["prefetch_orders"],
        "prewarm_indexes": ["prefetch_orders_date_idx"],
        "prewarm_tables": ["prefetch_orders"],
    }),
    ("A5", {
        "name": "Sparse - 1 TID per block (adversarial)",
        "sql": """
            SELECT id, category FROM prefetch_sparse
            WHERE category BETWEEN 10 AND 20
        """,
        "evict": ["prefetch_sparse"],
        "prewarm_indexes": ["prefetch_sparse_cat_idx"],
        "prewarm_tables": ["prefetch_sparse"],
        "setup_table": "prefetch_sparse",
    }),
    ("A6", {
        "name": "NL inner with LIMIT 1 (adversarial)",
        "sql": """
            SELECT c.customer_name, (
              SELECT amount FROM prefetch_orders o
              WHERE o.customer_id = c.customer_id
              ORDER BY o.order_date DESC
              LIMIT 1
            )
            FROM prefetch_customers c
            WHERE c.customer_id BETWEEN 1000 AND 2000
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
    }),
    ("A7", {
        "name": "Index Only Scan (adversarial)",
        "sql": """
            SELECT c.customer_id, c.customer_name
            FROM prefetch_customers c
            WHERE NOT EXISTS (
              SELECT 1 FROM prefetch_orders o
              WHERE o.customer_id = c.customer_id
                AND o.order_date BETWEEN '2023-12-01' AND '2023-12-31'
            )
        """,
        "evict": ["prefetch_orders", "prefetch_customers"],
        "prewarm_indexes": ["prefetch_orders_cust_date_idx", "prefetch_customers_pkey"],
        "prewarm_tables": ["prefetch_orders", "prefetch_customers"],
    }),
])


# --- Data Loading SQL ---

DATA_LOADING_SQL = """
-- Create extensions
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
CREATE EXTENSION IF NOT EXISTS pg_buffercache;

-- Drop existing tables
DROP TABLE IF EXISTS prefetch_orders CASCADE;
DROP TABLE IF EXISTS prefetch_customers CASCADE;
DROP TABLE IF EXISTS prefetch_products CASCADE;
DROP TABLE IF EXISTS prefetch_sequential CASCADE;
DROP TABLE IF EXISTS prefetch_sparse CASCADE;

-- Main fact table: ~50M rows with low fillfactor
CREATE UNLOGGED TABLE prefetch_orders (
  order_id bigint,
  customer_id int,
  product_id int,
  order_date date,
  region_id int,
  amount numeric(10,2)
) WITH (fillfactor = 40);

-- Dimension tables
CREATE UNLOGGED TABLE prefetch_customers (
  customer_id int PRIMARY KEY,
  region_id int,
  customer_name text
);

CREATE UNLOGGED TABLE prefetch_products (
  product_id int PRIMARY KEY,
  category_id int,
  product_name text
);

-- Load customers (100K)
INSERT INTO prefetch_customers (customer_id, region_id, customer_name)
SELECT i, (i % 20) + 1, 'Customer_' || i
FROM generate_series(1, 100000) i;

-- Load products (10K)
INSERT INTO prefetch_products (product_id, category_id, product_name)
SELECT i, (i % 50) + 1, 'Product_' || i
FROM generate_series(1, 10000) i;

-- Set deterministic seed
SELECT setseed(0.5);

-- Load orders with controlled scatter pattern
INSERT INTO prefetch_orders (order_id, customer_id, product_id, order_date, region_id, amount)
SELECT
  row_number() over () as order_id,
  customer_id,
  product_id,
  order_date,
  (customer_id % 20) + 1 as region_id,
  (random() * 1000)::numeric(10,2) as amount
FROM (
  SELECT
    ((g.i - 1) % 100000) + 1 as customer_id,
    ((g.i - 1) % 10000) + 1 as product_id,
    '2023-01-01'::date + ((g.i - 1) % 730) as order_date
  FROM generate_series(1, 50000000) g(i)
  ORDER BY (g.i / 32) + (random() * 4 - 2)::int
) sub;

-- Create indexes
CREATE INDEX prefetch_orders_cust_date_idx
  ON prefetch_orders(customer_id, order_date)
  WITH (deduplicate_items=off);

CREATE INDEX prefetch_orders_date_idx
  ON prefetch_orders(order_date)
  WITH (deduplicate_items=off);

CREATE INDEX prefetch_orders_prod_idx
  ON prefetch_orders(product_id)
  WITH (deduplicate_items=off);

CREATE INDEX prefetch_orders_id_idx
  ON prefetch_orders(order_id)
  WITH (deduplicate_items=off);

-- VACUUM FREEZE ANALYZE
VACUUM FREEZE ANALYZE prefetch_orders;
VACUUM FREEZE ANALYZE prefetch_customers;
VACUUM FREEZE ANALYZE prefetch_products;

-- Adversarial table: sequential heap access
CREATE UNLOGGED TABLE prefetch_sequential (
  id bigint,
  val1 int,
  val2 text
);
INSERT INTO prefetch_sequential
SELECT i, i % 1000, 'value_' || i
FROM generate_series(1, 500000) i;
CREATE INDEX prefetch_sequential_idx ON prefetch_sequential(id);
VACUUM ANALYZE prefetch_sequential;

-- Adversarial table: sparse (1 TID per block)
CREATE UNLOGGED TABLE prefetch_sparse (
  id bigint,
  category int,
  padding text
);
ALTER TABLE prefetch_sparse ALTER COLUMN padding SET STORAGE plain;
INSERT INTO prefetch_sparse
SELECT i, (i % 50) + 1, repeat('x', 4000)
FROM generate_series(1, 50000) i
ORDER BY random();
CREATE INDEX prefetch_sparse_cat_idx ON prefetch_sparse(category);
VACUUM ANALYZE prefetch_sparse;

CHECKPOINT;
"""


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Prefetch benchmark for PostgreSQL index scan prefetching.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s                         # Run all queries, uncached, 3 runs
    %(prog)s --cached                # Run with data prewarmed
    %(prog)s --queries Q1,Q2,A1      # Run specific queries
    %(prog)s --runs 5                # 5 runs per query
    %(prog)s --skip-load             # Skip data loading verification
        """
    )
    parser.add_argument(
        "--cached",
        action="store_true",
        help="Run in cached mode (prewarm all relations instead of just indexes)"
    )
    parser.add_argument(
        "--queries",
        type=str,
        default=None,
        help="Comma-separated list of queries to run (e.g., Q1,Q2,A1). Default: all"
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per query (default: 3)"
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        dest="skip_load",
        help="Skip data loading verification (assume tables exist with correct data)"
    )
    parser.add_argument(
        "--benchmark-cpu",
        type=int,
        default=BENCHMARK_CPU,
        help=f"CPU core to pin PostgreSQL backend to (default: {BENCHMARK_CPU})"
    )
    return parser.parse_args()


def get_git_hash(source_dir):
    """Get the current git commit hash for a source directory."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=source_dir,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "unknown"


def get_pg_version(conn_details):
    """Get PostgreSQL version string."""
    try:
        conn = psycopg.connect(**conn_details, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
        conn.close()
        return version
    except Exception as e:
        return f"error: {e}"


def start_server(pg_bin_dir, pg_name, pg_data_dir, conn_details):
    """Start a PostgreSQL server and wait for it to be ready."""
    pg_ctl_path = os.path.join(pg_bin_dir, "pg_ctl")
    log_file = os.path.join(OUTPUT_DIR, f"{pg_name}.postgres_log")

    # Ensure server is stopped before we start
    result = subprocess.run(
        [pg_ctl_path, "status", "-D", pg_data_dir],
        capture_output=True,
        check=False
    )
    if result.returncode == 0:
        print(f"{pg_name}: Server is already running. Stopping it...")
        subprocess.run([pg_ctl_path, "stop", "-D", pg_data_dir, "-m", "fast"], check=True)
        time.sleep(2)

    # Start the server
    print(f"Starting {pg_name} PostgreSQL server (port {conn_details.get('port', 'default')})...")
    start_options = f"-p {conn_details['port']}" if 'port' in conn_details else ""

    result = subprocess.run(
        [pg_ctl_path, "start",
         "-o", "--autovacuum=off",
         "-D", pg_data_dir,
         "-l", log_file,
         "-o", start_options],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Error: Failed to start {pg_name} server")
        print(f"stdout: {result.stdout}")
        print(f"stderr: {result.stderr}")
        sys.exit(1)

    # Wait for the server to be ready
    print(f"Waiting for {pg_name} server to accept connections...")
    for attempt in range(15):
        try:
            conn = psycopg.connect(**conn_details, connect_timeout=2)
            conn.close()
            print(f"{pg_name} server started successfully.")
            return
        except (psycopg.OperationalError, psycopg.DatabaseError):
            if attempt == 14:
                print(f"Error: {pg_name} server failed to start after 15 attempts")
                sys.exit(1)
            time.sleep(0.5 if attempt < 5 else 1)


def stop_server(pg_bin_dir, pg_data_dir):
    """Stop a PostgreSQL server."""
    pg_ctl_path = os.path.join(pg_bin_dir, "pg_ctl")
    subprocess.run([pg_ctl_path, "stop", "-D", pg_data_dir, "-m", "fast"], check=False)


def verify_data(conn_details, skip_load=False):
    """
    Verify that tables exist with expected row counts.
    Returns True if data is valid, False if reload is needed.
    """
    if skip_load:
        print("Skipping data verification (--skip-load)")
        return True

    try:
        conn = psycopg.connect(**conn_details)

        # Check if main table exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'prefetch_orders' AND relkind = 'r'
                )
            """)
            if not cur.fetchone()[0]:
                print("Table prefetch_orders does not exist")
                conn.close()
                return False

        # Check row counts
        checks = [
            ("prefetch_orders", EXPECTED_ORDERS),
            ("prefetch_customers", EXPECTED_CUSTOMERS),
            ("prefetch_products", EXPECTED_PRODUCTS),
        ]

        for table, expected in checks:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {table}")
                actual = cur.fetchone()[0]
                tolerance = expected * ROW_COUNT_TOLERANCE
                if abs(actual - expected) > tolerance:
                    print(f"Table {table}: expected ~{expected:,} rows, got {actual:,}")
                    conn.close()
                    return False
                print(f"Table {table}: {actual:,} rows (expected ~{expected:,}) ✓")

        # Check adversarial tables exist
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'prefetch_sequential' AND relkind = 'r'
                )
            """)
            if not cur.fetchone()[0]:
                print("Adversarial table prefetch_sequential does not exist")
                conn.close()
                return False

            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'prefetch_sparse' AND relkind = 'r'
                )
            """)
            if not cur.fetchone()[0]:
                print("Adversarial table prefetch_sparse does not exist")
                conn.close()
                return False

        conn.close()
        return True

    except Exception as e:
        print(f"Error verifying data: {e}")
        return False


def load_data(conn_details):
    """Load benchmark data into the database."""
    print("\n" + "=" * 50)
    print("Loading benchmark data...")
    print("This will take several minutes for 50M rows.")
    print("=" * 50 + "\n")

    conn = psycopg.connect(**conn_details)
    conn.autocommit = True

    # Parse SQL into individual statements, handling comments properly
    statements = []
    current_stmt = []
    for line in DATA_LOADING_SQL.split('\n'):
        stripped = line.strip()
        # Skip pure comment lines and empty lines when building statements
        if stripped.startswith('--') or not stripped:
            continue
        current_stmt.append(line)
        if stripped.endswith(';'):
            statements.append('\n'.join(current_stmt))
            current_stmt = []

    for statement in statements:
        statement = statement.strip()
        if not statement:
            continue
        try:
            # Print progress for long operations BEFORE executing
            if 'INSERT INTO prefetch_orders' in statement and 'generate_series(1, 50000000)' in statement:
                print("Loading 50M orders... (this takes a while)")
            elif 'CREATE INDEX' in statement:
                idx_match = re.search(r'CREATE INDEX (\S+)', statement)
                idx_name = idx_match.group(1) if idx_match else "index"
                print(f"Creating index {idx_name}...")
            elif 'VACUUM' in statement:
                print(f"Running {statement.split()[0]} {statement.split()[1] if len(statement.split()) > 1 else ''}...")

            with conn.cursor() as cur:
                cur.execute(statement)

        except Exception as e:
            print(f"Error executing: {statement[:80]}...")
            print(f"Error: {e}")
            conn.close()
            sys.exit(1)

    conn.close()
    print("Data loading complete.")


def clear_os_cache():
    """Clear the OS page cache."""
    result = subprocess.run(
        ["sudo", "clear_cache.sh"],
        capture_output=True,
        check=False
    )
    if result.returncode != 0:
        print("Warning: Failed to clear OS cache")


def evict_relations(conn, relations):
    """Evict relations from PostgreSQL buffer cache."""
    with conn.cursor() as cur:
        for rel in relations:
            try:
                cur.execute(f"SELECT pg_buffercache_evict_relation('{rel}')")
            except Exception as e:
                print(f"Warning: Failed to evict {rel}: {e}")


def prewarm_relations(conn, relations):
    """Prewarm relations into PostgreSQL buffer cache."""
    with conn.cursor() as cur:
        for rel in relations:
            try:
                cur.execute(f"SELECT pg_prewarm('{rel}')")
            except Exception as e:
                print(f"Warning: Failed to prewarm {rel}: {e}")


def set_gucs(conn, gucs, is_master=False, prefetch_setting=None):
    """Set GUCs for query execution."""
    with conn.cursor() as cur:
        # Base GUCs for all configurations
        cur.execute("SET enable_bitmapscan = off")
        cur.execute("SET enable_seqscan = off")
        cur.execute("SET max_parallel_workers_per_gather = 0")

        # Set prefetch GUC only on patch (not master)
        if not is_master and prefetch_setting is not None:
            cur.execute(f"SET enable_indexscan_prefetch = {prefetch_setting}")

        # Query-specific GUCs
        for guc, value in gucs.items():
            cur.execute(f"SET {guc} = {value}")


def reset_gucs(conn, gucs):
    """Reset query-specific GUCs to defaults."""
    with conn.cursor() as cur:
        for guc in gucs:
            cur.execute(f"RESET {guc}")


def extract_execution_time(explain_output):
    """Extract execution time from EXPLAIN ANALYZE output."""
    for line in explain_output:
        match = re.search(r'Execution Time: ([\d.]+) ms', line[0])
        if match:
            return float(match.group(1))
    return None


def pin_backend(pid, cpu):
    """Pin a backend process to a specific CPU."""
    try:
        result = subprocess.run(
            ["taskset", "-cp", str(cpu), str(pid)],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0:
            print(f"Pinned backend PID {pid} to CPU {cpu}")

        # Try RT scheduling
        result = subprocess.run(
            ["sudo", "chrt", "-f", "-p", "1", str(pid)],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0:
            print(f"Set backend PID {pid} to SCHED_FIFO")
    except Exception as e:
        print(f"Warning: Could not pin backend: {e}")


def run_query(conn, query_def, cached_mode, is_master, prefetch_setting, benchmark_cpu):
    """
    Run a single query with proper cache preparation.
    Returns (execution_time_ms, explain_output_str) tuple.
    """
    # Cache preparation
    if cached_mode:
        # Prewarm everything
        prewarm_relations(conn, query_def.get("prewarm_indexes", []))
        prewarm_relations(conn, query_def.get("prewarm_tables", []))
    else:
        # Uncached: evict heap, prewarm indexes, clear OS cache
        evict_relations(conn, query_def.get("evict", []))
        prewarm_relations(conn, query_def.get("prewarm_indexes", []))
        clear_os_cache()

    # Set GUCs
    gucs = query_def.get("gucs", {})
    set_gucs(conn, gucs, is_master=is_master, prefetch_setting=prefetch_setting)

    # Special handling for warmup queries (A3)
    if query_def.get("warmup_query") and not cached_mode:
        with conn.cursor() as cur:
            cur.execute(query_def["sql"])
            cur.execute(query_def["sql"])

    # Execute with EXPLAIN ANALYZE
    sql = query_def["sql"].strip()
    explain_sql = f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) {sql}"

    with conn.cursor() as cur:
        cur.execute(explain_sql)
        result = cur.fetchall()

    # Reset query-specific GUCs
    if gucs:
        reset_gucs(conn, gucs)

    # Format EXPLAIN output as string
    explain_output = "\n".join(row[0] for row in result)

    # Extract execution time
    exec_time = extract_execution_time(result)
    if exec_time is None:
        print("Warning: Could not extract execution time from EXPLAIN output")
        return None, explain_output

    return exec_time, explain_output


def run_benchmark(args):
    """Run the benchmark."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Parse query selection
    if args.queries:
        selected_queries = [q.strip().upper() for q in args.queries.split(",")]
        for q in selected_queries:
            if q not in QUERIES:
                print(f"Error: Unknown query '{q}'. Available: {', '.join(QUERIES.keys())}")
                sys.exit(1)
    else:
        selected_queries = list(QUERIES.keys())

    print(f"\n{'=' * 60}")
    print("Prefetch Benchmark")
    print(f"{'=' * 60}")
    print(f"Mode: {'cached' if args.cached else 'uncached'}")
    print(f"Queries: {', '.join(selected_queries)}")
    print(f"Runs per query: {args.runs}")
    print(f"{'=' * 60}\n")

    # Get git hashes
    master_hash = get_git_hash(MASTER_SOURCE_DIR)
    patch_hash = get_git_hash(PATCH_SOURCE_DIR)
    print(f"Master git hash: {master_hash}")
    print(f"Patch git hash: {patch_hash}")

    # Verify/load data on each server (one at a time due to memory constraints)
    print("\n--- Verifying data on master ---")
    start_server(MASTER_BIN, "master", MASTER_DATA_DIR, MASTER_CONN)
    if not verify_data(MASTER_CONN, args.skip_load):
        print("Loading data on master...")
        load_data(MASTER_CONN)
    master_version = get_pg_version(MASTER_CONN)
    stop_server(MASTER_BIN, MASTER_DATA_DIR)
    time.sleep(2)

    print("\n--- Verifying data on patch ---")
    start_server(PATCH_BIN, "patch", PATCH_DATA_DIR, PATCH_CONN)
    if not verify_data(PATCH_CONN, args.skip_load):
        print("Loading data on patch...")
        load_data(PATCH_CONN)
    patch_version = get_pg_version(PATCH_CONN)
    stop_server(PATCH_BIN, PATCH_DATA_DIR)
    time.sleep(2)

    # Results storage
    results = {
        "timestamp": datetime.now().isoformat(),
        "master_hash": master_hash,
        "patch_hash": patch_hash,
        "master_version": master_version,
        "patch_version": patch_version,
        "mode": "cached" if args.cached else "uncached",
        "runs": args.runs,
        "queries": {},
    }

    # Initialize query results structure
    for query_id in selected_queries:
        query_def = QUERIES[query_id]
        results["queries"][query_id] = {
            "name": query_def["name"],
            "master": {"times": [], "avg": None, "min": None, "max": None, "explain": None},
            "patch_off": {"times": [], "avg": None, "min": None, "max": None, "explain": None},
            "patch_on": {"times": [], "avg": None, "min": None, "max": None, "explain": None},
        }

    # Run all queries on master
    print(f"\n{'=' * 60}")
    print("Running all queries on MASTER")
    print(f"{'=' * 60}")
    master_start_time = time.time()
    start_server(MASTER_BIN, "master", MASTER_DATA_DIR, MASTER_CONN)
    try:
        master_conn = psycopg.connect(**MASTER_CONN)
        pin_backend(master_conn.info.backend_pid, args.benchmark_cpu)
        with master_conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_buffercache")

        for query_id in selected_queries:
            query_def = QUERIES[query_id]
            print(f"\n{query_id}: {query_def['name']} ({args.runs} runs)...")
            for run in range(args.runs):
                exec_time, explain_output = run_query(
                    master_conn, query_def, args.cached,
                    is_master=True, prefetch_setting=None,
                    benchmark_cpu=args.benchmark_cpu
                )
                if exec_time is not None:
                    results["queries"][query_id]["master"]["times"].append(exec_time)
                    # Save the last run's explain output
                    results["queries"][query_id]["master"]["explain"] = explain_output
                    print(f"  Run {run + 1}: {exec_time:.3f} ms")

        master_conn.close()
    finally:
        stop_server(MASTER_BIN, MASTER_DATA_DIR)
        time.sleep(2)
    master_end_time = time.time()

    # Run all queries on patch (both prefetch=off and prefetch=on)
    print(f"\n{'=' * 60}")
    print("Running all queries on PATCH")
    print(f"{'=' * 60}")
    patch_start_time = time.time()
    start_server(PATCH_BIN, "patch", PATCH_DATA_DIR, PATCH_CONN)
    try:
        patch_conn = psycopg.connect(**PATCH_CONN)
        pin_backend(patch_conn.info.backend_pid, args.benchmark_cpu)
        with patch_conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_buffercache")

        for query_id in selected_queries:
            query_def = QUERIES[query_id]

            # Run with prefetch OFF
            print(f"\n{query_id}: {query_def['name']} (prefetch=off, {args.runs} runs)...")
            for run in range(args.runs):
                exec_time, explain_output = run_query(
                    patch_conn, query_def, args.cached,
                    is_master=False, prefetch_setting="off",
                    benchmark_cpu=args.benchmark_cpu
                )
                if exec_time is not None:
                    results["queries"][query_id]["patch_off"]["times"].append(exec_time)
                    # Save the last run's explain output
                    results["queries"][query_id]["patch_off"]["explain"] = explain_output
                    print(f"  Run {run + 1}: {exec_time:.3f} ms")

            # Run with prefetch ON
            print(f"\n{query_id}: {query_def['name']} (prefetch=on, {args.runs} runs)...")
            for run in range(args.runs):
                exec_time, explain_output = run_query(
                    patch_conn, query_def, args.cached,
                    is_master=False, prefetch_setting="on",
                    benchmark_cpu=args.benchmark_cpu
                )
                if exec_time is not None:
                    results["queries"][query_id]["patch_on"]["times"].append(exec_time)
                    # Save the last run's explain output
                    results["queries"][query_id]["patch_on"]["explain"] = explain_output
                    print(f"  Run {run + 1}: {exec_time:.3f} ms")

        patch_conn.close()
    finally:
        stop_server(PATCH_BIN, PATCH_DATA_DIR)
        time.sleep(2)
    patch_end_time = time.time()

    # Calculate statistics and print summaries
    print(f"\n{'=' * 60}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 60}")

    for query_id in selected_queries:
        query_results = results["queries"][query_id]

        # Calculate statistics
        for config in ["master", "patch_off", "patch_on"]:
            times = query_results[config]["times"]
            if times:
                query_results[config]["avg"] = mean(times)
                query_results[config]["min"] = min(times)
                query_results[config]["max"] = max(times)

        # Print summary for this query
        master_avg = query_results["master"]["avg"]
        patch_off_avg = query_results["patch_off"]["avg"]
        patch_on_avg = query_results["patch_on"]["avg"]

        # ANSI bold escape codes
        BOLD = "\033[1m"
        RESET = "\033[0m"

        print(f"\n{BOLD}{query_id}: {query_results['name']}{RESET}")
        if master_avg:
            print(f"  master:               {master_avg:10.3f} ms "
                  f"(min={query_results['master']['min']:.3f}, max={query_results['master']['max']:.3f})")
        if patch_off_avg and master_avg:
            ratio_off = patch_off_avg / master_avg
            print(f"  patch (prefetch=off): {patch_off_avg:10.3f} ms "
                  f"(min={query_results['patch_off']['min']:.3f}, max={query_results['patch_off']['max']:.3f}) "
                  f"[{BOLD}{ratio_off:.3f}x{RESET} vs master]")
        if patch_on_avg and master_avg:
            ratio_on = patch_on_avg / master_avg
            print(f"  patch (prefetch=on):  {patch_on_avg:10.3f} ms "
                  f"(min={query_results['patch_on']['min']:.3f}, max={query_results['patch_on']['max']:.3f}) "
                  f"[{BOLD}{ratio_on:.3f}x{RESET} vs master]")

        # Print EXPLAIN ANALYZE outputs
        print()
        if query_results["master"]["explain"]:
            print("  master EXPLAIN ANALYZE:")
            for line in query_results["master"]["explain"].split('\n'):
                print(f"    {line}")
        if query_results["patch_off"]["explain"]:
            print("  patch (prefetch=off) EXPLAIN ANALYZE:")
            for line in query_results["patch_off"]["explain"].split('\n'):
                print(f"    {line}")
        if query_results["patch_on"]["explain"]:
            print("  patch (prefetch=on) EXPLAIN ANALYZE:")
            for line in query_results["patch_on"]["explain"].split('\n'):
                print(f"    {line}")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file = os.path.join(OUTPUT_DIR, f"benchmark_{timestamp}.json")
    txt_file = os.path.join(OUTPUT_DIR, f"benchmark_{timestamp}.txt")

    # Save JSON
    with open(json_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {json_file}")

    # Save human-readable text
    with open(txt_file, "w") as f:
        f.write("=" * 70 + "\n")
        f.write("Prefetch Benchmark Results\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Timestamp: {results['timestamp']}\n")
        f.write(f"Master git hash: {results['master_hash']}\n")
        f.write(f"Patch git hash: {results['patch_hash']}\n")
        f.write(f"Mode: {results['mode']}\n")
        f.write(f"Runs per query: {results['runs']}\n\n")

        for query_id, qr in results["queries"].items():
            f.write(f"\n{query_id}: {qr['name']}\n")
            f.write("-" * 50 + "\n")

            master_avg = qr["master"]["avg"]
            patch_off_avg = qr["patch_off"]["avg"]
            patch_on_avg = qr["patch_on"]["avg"]

            if master_avg:
                f.write(f"  master:               {master_avg:10.3f} ms "
                        f"(min={qr['master']['min']:.3f}, max={qr['master']['max']:.3f})\n")
            if patch_off_avg and master_avg:
                ratio_off = patch_off_avg / master_avg
                f.write(f"  patch (prefetch=off): {patch_off_avg:10.3f} ms "
                        f"(min={qr['patch_off']['min']:.3f}, max={qr['patch_off']['max']:.3f}) "
                        f"[{ratio_off:.3f}x vs master]\n")
            if patch_on_avg and master_avg:
                ratio_on = patch_on_avg / master_avg
                f.write(f"  patch (prefetch=on):  {patch_on_avg:10.3f} ms "
                        f"(min={qr['patch_on']['min']:.3f}, max={qr['patch_on']['max']:.3f}) "
                        f"[{ratio_on:.3f}x vs master]\n")

            # Write EXPLAIN ANALYZE outputs
            f.write("\n")
            if qr["master"]["explain"]:
                f.write("  master EXPLAIN ANALYZE:\n")
                for line in qr["master"]["explain"].split('\n'):
                    f.write(f"    {line}\n")
            if qr["patch_off"]["explain"]:
                f.write("  patch (prefetch=off) EXPLAIN ANALYZE:\n")
                for line in qr["patch_off"]["explain"].split('\n'):
                    f.write(f"    {line}\n")
            if qr["patch_on"]["explain"]:
                f.write("  patch (prefetch=on) EXPLAIN ANALYZE:\n")
                for line in qr["patch_on"]["explain"].split('\n'):
                    f.write(f"    {line}\n")

    print(f"Results saved to: {txt_file}")

    # Update latest symlink
    latest_link = os.path.join(OUTPUT_DIR, "latest.txt")
    if os.path.exists(latest_link):
        os.remove(latest_link)
    os.symlink(os.path.basename(txt_file), latest_link)

    # Print total run times
    master_duration = master_end_time - master_start_time
    patch_duration = patch_end_time - patch_start_time
    total_duration = master_duration + patch_duration

    print(f"\n{'=' * 60}")
    print("BENCHMARK RUN TIMES (excluding data loading)")
    print(f"{'=' * 60}")
    print(f"  Master:  {master_duration:10.1f} seconds ({master_duration / 60:.1f} minutes)")
    print(f"  Patch:   {patch_duration:10.1f} seconds ({patch_duration / 60:.1f} minutes)")
    print(f"  Total:   {total_duration:10.1f} seconds ({total_duration / 60:.1f} minutes)")

    # Collect all patch runs with their ratios vs master
    # Each patch configuration (prefetch=off, prefetch=on) is treated independently
    all_ratios = []
    for query_id in selected_queries:
        qr = results["queries"][query_id]
        master_avg = qr["master"]["avg"]
        if not master_avg:
            continue

        # patch (prefetch=off)
        patch_off_avg = qr["patch_off"]["avg"]
        if patch_off_avg:
            ratio = patch_off_avg / master_avg
            all_ratios.append({
                "query_id": query_id,
                "name": qr["name"],
                "config": "prefetch=off",
                "ratio": ratio,
                "master_ms": master_avg,
                "patch_ms": patch_off_avg,
            })

        # patch (prefetch=on)
        patch_on_avg = qr["patch_on"]["avg"]
        if patch_on_avg:
            ratio = patch_on_avg / master_avg
            all_ratios.append({
                "query_id": query_id,
                "name": qr["name"],
                "config": "prefetch=on",
                "ratio": ratio,
                "master_ms": master_avg,
                "patch_ms": patch_on_avg,
            })

    # Sort by ratio: improvements are < 1.0, regressions are > 1.0
    # Top improvements: lowest ratios (fastest vs master)
    # Top regressions: highest ratios (slowest vs master)
    sorted_by_ratio = sorted(all_ratios, key=lambda x: x["ratio"])

    BOLD = "\033[1m"
    RESET = "\033[0m"

    print(f"\n{'=' * 60}")
    print("TOP 5 IMPROVEMENTS vs MASTER")
    print(f"{'=' * 60}")
    for entry in sorted_by_ratio[:5]:
        print(f"  {entry['query_id']} ({entry['config']}): {entry['name']}")
        print(f"    {BOLD}{entry['ratio']:.3f}x{RESET} - master: {entry['master_ms']:.3f} ms, patch: {entry['patch_ms']:.3f} ms")

    print(f"\n{'=' * 60}")
    print("TOP 5 REGRESSIONS vs MASTER")
    print(f"{'=' * 60}")
    # Take top 5 from the end (highest ratios)
    regressions = sorted_by_ratio[-5:][::-1]  # Reverse to show worst first
    for entry in regressions:
        print(f"  {entry['query_id']} ({entry['config']}): {entry['name']}")
        print(f"    {BOLD}{entry['ratio']:.3f}x{RESET} - master: {entry['master_ms']:.3f} ms, patch: {entry['patch_ms']:.3f} ms")


def main():
    args = parse_arguments()
    run_benchmark(args)


if __name__ == "__main__":
    main()

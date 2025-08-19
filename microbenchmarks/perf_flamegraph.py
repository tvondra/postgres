#!/usr/bin/env python3

import os
import psycopg
import shutil
import random
import signal
import subprocess
import sys
import time

# --- Configuration ---

# Paths to the PostgreSQL binaries for the two versions you want to compare.
MASTER_BIN="/mnt/nvme/postgresql/master/install_meson_rc/bin"
PATCH_BIN="/mnt/nvme/postgresql/patch/install_meson_rc/bin"

# Data directories for the two PostgreSQL instances.
MASTER_DATA_DIR="/mnt/nvme/postgresql/master/data"
PATCH_DATA_DIR="/mnt/nvme/postgresql/patch/data"

# Connection details for the two PostgreSQL instances.
# Using a dictionary for easier access.
MASTER_CONN_DETAILS = {
    "dbname": "regression",
    "user": "pg",
    "host": "/tmp",
    "port": 5555 # IMPORTANT: Ensure the two versions run on different ports
}
PATCH_CONN_DETAILS = {
    "dbname": "regression",
    "user": "pg",
    "host": "/tmp", # Use socket for local connections
    "port": 5432
}

DIFF_FLAMEGRAPH_TITLE="master versus patch"

# The SQL query you want to profile.
# SQL_QUERY="select * from pgbench_accounts where aid = %s;"
SQL_QUERY="select count(*) from pgbench_accounts a join pgbench_branches b on a.bid = b.bid;"

# Number of times to run the query to ensure it's captured by perf.
# QUERY_REPETITIONS=500_000
QUERY_REPETITIONS=5

# The frequency of 'perf' sampling.
PERF_FREQUENCY=9999

# The user that the PostgreSQL server runs as.
PG_USER = "pg"

# --- Script variables ---
FLAMEGRAPH_DIR="/home/pg/code/FlameGraph"
OUTPUT_DIR = "output_perf_flamegraph"

def check_dependencies():
    """Check if required command-line tools are available."""
    for cmd in ["perf", "git"]:
        if not shutil.which(cmd):
            print(f"Error: Command '{cmd}' not found. Please install it.")
            sys.exit(1)

def clone_flamegraph():
    """Clone the FlameGraph repository if it doesn't exist."""
    if not os.path.isdir(FLAMEGRAPH_DIR):
        raise Exception("no FlameGraph repository")
    else:
        print("FlameGraph repository already exists.")

def profile_postgres(pg_bin_dir, pg_name, pg_data_dir, conn_details, output_file):
    """Starts, profiles, and stops a PostgreSQL instance."""
    pg_ctl_path = os.path.join(pg_bin_dir, "pg_ctl")

    print(f"--- Profiling {pg_name} ---")

    # Ensure server is stopped before we start
    if subprocess.run([pg_ctl_path, "status", "-D", pg_data_dir]).returncode == 0:
        print("Server is already running. Stopping it...")
        subprocess.run([pg_ctl_path, "stop", "-D", pg_data_dir, "-m", "fast"], check=True)
        time.sleep(2)

    # Start the server
    print(f"Starting {pg_name} PostgreSQL server...")
    log_file = os.path.join(OUTPUT_DIR, f"pg_{os.path.basename(pg_bin_dir)}.log")

    # Add port to the options if it's in the connection details
    start_options = f"-p {conn_details['port']}" if 'port' in conn_details else ""

    subprocess.run(
        [pg_ctl_path, "start",
         "-o", "--autovacuum=off",
         # "-o", "--debug_io_direct=data",
         "-D", pg_data_dir,
         "-l", log_file, "-o",
         start_options],
        check=True
    )

    # Wait for the server to be ready
    print("Waiting for server to accept connections...")
    for _ in range(10):
        try:
            conn = psycopg.connect(**conn_details)
            conn.close()
            print("Server started successfully.")
            break
        except psycopg.OperationalError:
            time.sleep(1)
    else:
        print("Error: Server failed to start or become available.")
        sys.exit(1)

    conn = None
    try:
        # Connect to the database to get the backend PID
        conn = psycopg.connect(**conn_details, prepare_threshold=0)

        backend_pid = conn.info.backend_pid
        print(f"Successfully connected. Backend PID is: {backend_pid}")

        print(f"Prewarming...")
        with conn.cursor() as cursor:
            # if pg_name == "patch":
            #     cursor.execute("set enable_indexscan_prefetch=off;")
            cursor.execute("set enable_seqscan=off;")
            cursor.execute("set enable_indexonlyscan=off;")
            cursor.execute("set enable_bitmapscan=off;")
            cursor.execute("set enable_hashjoin=off;")
            cursor.execute("set enable_memoize=off;")
            cursor.execute("set enable_mergejoin=off;")
            cursor.execute("set max_parallel_workers_per_gather = 0;")
            cursor.execute("create extension if not exists pg_prewarm;")
            cursor.execute("select pg_prewarm('pgbench_accounts');")
            cursor.execute("select pg_prewarm('pgbench_accounts_pkey');")
            cursor.execute("select pg_prewarm('pgbench_branches');")
            cursor.execute("select pg_prewarm('pgbench_branches_pkey');")
        print(f"Finished prewarming")

        # Start perf in the background, targeting the specific backend PID.
        # It will run until we explicitly stop it.
        print(f"Starting perf on PID {backend_pid}...")
        perf_command = [
            "perf", "record",
            "-F", str(PERF_FREQUENCY),
            # "-e", "alignment-faults",
            # "-e", "branch-instructions",
            # "-e", "branch-misses",
            # "-e", "branches",
            # "-e", "cache-misses",
            # "-e", "cache-references",
            # "-e", "cgroup-switches",
            # "-e", "context-switches",
            # "-e", "cpu-clock",
            # "-e", "cpu-cycles",
            # "-e", "cpu-migrations",
            # "-e", "instructions",
            # "-e", "major-faults",
            # "-e", "minor-faults",
            # "-e", "page-faults",
            # "-e", "stalled-cycles-frontend",
            # "-e", "task-clock",
            "-p", str(backend_pid),
            "-o",  OUTPUT_DIR + "/" + pg_name,
            "-g"
        ]
        perf_process = subprocess.Popen(perf_command)

        # Give perf a moment to initialize before starting the workload
        time.sleep(1)

        # Execute the query repeatedly in the same connection
        print(f"Executing the SQL query {QUERY_REPETITIONS} times...")
        start_time = time.time()
        with conn.cursor() as cursor:
            for i in range(QUERY_REPETITIONS):
                random.seed(42)
                # cursor.execute(query=SQL_QUERY, params=[random.randint(1,50_00_000)], prepare=True)
                cursor.execute(query=SQL_QUERY, prepare=True)
        end_time = time.time()
        print(f"Query loop finished in {end_time - start_time:.2f} seconds.")

        # Stop the perf process gracefully by sending SIGINT (like Ctrl+C)
        print("Stopping perf...")
        perf_process.send_signal(signal.SIGINT)

        # Wait for perf to terminate
        perf_process.wait()

        # Generate the stack trace file, normalizing the binary path
        print(f"Generating and normalizing stack trace file: {output_file}")

        # The sed expression will replace the full, version-specific path
        # with the generic name 'postgres', allowing difffolded.pl to match symbols.
        pg_executable_path = os.path.join(pg_bin_dir, "postgres")
        sed_expression = f"s|{pg_executable_path}|postgres|g"

        perf_script_process = subprocess.Popen(["perf", "script",
                                                "-i",  OUTPUT_DIR + "/" + pg_name,
                                                ], stdout=subprocess.PIPE)

        with open(output_file, "w") as f:
            sed_process = subprocess.run(
                ["sed", sed_expression],
                stdin=perf_script_process.stdout,
                stdout=f,
                check=True
            )
        perf_script_process.stdout.close()
        perf_script_process.wait()

    finally:
        # Ensure the connection is closed and the server is stopped
        if conn:
            conn.close()
        print("Stopping PostgreSQL server...")
        subprocess.run([pg_ctl_path, "stop", "-D", pg_data_dir, "-m", "fast"], check=True)
        print("----------------------------------------")

    # Return string of perf command for flamegraph --subtitle arg
    return ' '.join(perf_command)

def main():
    """Main execution flow."""
    check_dependencies()
    clone_flamegraph()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stacks_file_master = os.path.join(OUTPUT_DIR, "master.stacks")
    stacks_file_patch = os.path.join(OUTPUT_DIR, "patch.stacks")
    folded_file_master = os.path.join(OUTPUT_DIR, "master.folded")
    folded_file_patch = os.path.join(OUTPUT_DIR, "patch.folded")
    svg_file_diff = os.path.join(OUTPUT_DIR, "diff.svg")
    svg_file_master = os.path.join(OUTPUT_DIR, "master_flamegraph.svg")
    svg_file_patch = os.path.join(OUTPUT_DIR, "patch_flamegraph.svg")

    # Profile both PostgreSQL versions
    perf_command_master = profile_postgres(MASTER_BIN, "master",
                                           MASTER_DATA_DIR, MASTER_CONN_DETAILS,
                                           stacks_file_master)
    perf_command_patch = profile_postgres(PATCH_BIN, "patch",
                                          PATCH_DATA_DIR, PATCH_CONN_DETAILS,
                                          stacks_file_patch)

    # --- Generate Flame Graphs ---
    print("--- Generating Flame Graphs ---")

    # Fold stack traces
    print("Folding stack traces...")
    with open(folded_file_master, "w") as f1, open(folded_file_patch, "w") as f2:
        subprocess.run(
            [os.path.join(FLAMEGRAPH_DIR, "stackcollapse-perf.pl"), stacks_file_master],
            stdout=f1, check=True
        )
        subprocess.run(
            [os.path.join(FLAMEGRAPH_DIR, "stackcollapse-perf.pl"), stacks_file_patch],
            stdout=f2, check=True
        )

    print("Creating individual flame graph for master...")
    with open(svg_file_master, "w") as f_svg:
        subprocess.run(
                [
                    os.path.join(FLAMEGRAPH_DIR, "flamegraph.pl"),
                    "--title", "master",
                    "--subtitle", perf_command_master,
                    folded_file_master,
                    ],
                stdout=f_svg,
                check=True,
                )
    print(f"V1 flame graph created: {svg_file_master}")

    print("Creating individual flame graph for patch...")
    with open(svg_file_patch, "w") as f_svg:
        subprocess.run(
                [
                    os.path.join(FLAMEGRAPH_DIR, "flamegraph.pl"),
                    "--title", "patch",
                    "--subtitle", perf_command_patch,
                    folded_file_patch,
                    ],
                stdout=f_svg,
                check=True,
                )
    print(f"V2 flame graph created: {svg_file_patch}")

    # Create the differential SVG
    print("Creating the differential flame graph...")
    difffolded_cmd = [os.path.join(FLAMEGRAPH_DIR, "difffolded.pl"), folded_file_master, folded_file_patch]
    flamegraph_cmd = [
        os.path.join(FLAMEGRAPH_DIR, "flamegraph.pl"),
        "--title", DIFF_FLAMEGRAPH_TITLE,
        "--subtitle", perf_command_master + ", " + perf_command_patch,
    ]

    p1 = subprocess.Popen(difffolded_cmd, stdout=subprocess.PIPE)
    with open(svg_file_diff, "w") as f_svg:
        subprocess.run(flamegraph_cmd, stdin=p1.stdout, stdout=f_svg, check=True)
    p1.stdout.close()

    print(f"Differential flame graph created: {svg_file_diff}")
    print("Done.")

if __name__ == "__main__":
    main()

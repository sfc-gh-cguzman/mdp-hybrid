"""
MDP PLC Hybrid Table - Stress Test (Persistent Connection Pool)
Concurrent point lookups against MDP_PLC_ST_HYBRID on a standard warehouse.

Comparison benchmark against the interactive table (MDP_PLC_ST_IT + MDP_PLC_IWH).
Hybrid tables use B-tree PK index for O(1) lookups vs columnar partition pruning.

Usage:
    conda run -n mdp_interactive python tests/stress_test_hybrid.py
"""

import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import numpy as np
import snowflake.connector

# Configuration - uses named connection from ~/.snowflake/connections.toml
CONNECTION_NAME = "spark-connect"
WAREHOUSE = "COMPUTE_WH"

TABLE = "ISRG_D3_DB.CURATED.MDP_PLC_ST_HYBRID"
QUERY_TEMPLATE = f"SELECT * FROM {TABLE} WHERE CONSENSUS_UNIQUE_IDENTIFIER = %s"

# Max connections in the persistent pool (reused across all tiers)
MAX_POOL_SIZE = 100

# Test tiers matching customer format (same as interactive table test)
TEST_TIERS = [
    {"clients": 10, "requests": 10},
    {"clients": 20, "requests": 50},
    {"clients": 50, "requests": 100},
    {"clients": 100, "requests": 200},
    {"clients": 100, "requests": 500},
    {"clients": 100, "requests": 5000},
    {"clients": 100, "requests": 10000},
]

WARMUP_QUERIES = 20
ID_POOL_SIZE = 2000


@dataclass
class QueryResult:
    success: bool
    client_latency_ms: float
    query_id: str | None = None


def log(msg: str):
    print(msg, flush=True)


def get_connection():
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    conn.cursor().execute(f"USE WAREHOUSE {WAREHOUSE}")
    return conn


def open_connection_pool(n: int) -> list:
    """Open N connections in parallel at startup."""
    log(f"  Opening {n} persistent connections (parallel, max 20 concurrent)...")
    connections = [None] * n
    def _open(idx):
        connections[idx] = get_connection()
    with ThreadPoolExecutor(max_workers=min(n, 20)) as executor:
        list(executor.map(_open, range(n)))
    failed = sum(1 for c in connections if c is None)
    if failed:
        log(f"  WARNING: {failed} connections failed to open")
    return [c for c in connections if c is not None]


def close_pool(connections: list):
    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass


def fetch_id_pool(conn, size: int) -> list[int]:
    """Pre-fetch a pool of valid CONSENSUS_UNIQUE_IDENTIFIER values."""
    cur = conn.cursor()
    cur.execute(
        f"SELECT CONSENSUS_UNIQUE_IDENTIFIER FROM {TABLE} SAMPLE ({size} ROWS)"
    )
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids


def run_single_query(conn, lookup_id: int) -> QueryResult:
    """Execute a single point lookup and measure client-side latency."""
    cur = conn.cursor()
    start = time.perf_counter()
    try:
        cur.execute(QUERY_TEMPLATE, (lookup_id,))
        cur.fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000
        query_id = cur.sfqid
        return QueryResult(success=True, client_latency_ms=elapsed_ms, query_id=query_id)
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        log(f"  ERROR: {e}")
        return QueryResult(success=False, client_latency_ms=elapsed_ms)
    finally:
        cur.close()


def worker(conn, ids_to_query: list[int]) -> list[QueryResult]:
    """Worker function - runs assigned queries on a persistent connection."""
    results = []
    for lookup_id in ids_to_query:
        results.append(run_single_query(conn, lookup_id))
    return results


def run_tier(
    pool: list, clients: int, total_requests: int, id_pool: list[int]
) -> tuple[list[QueryResult], float]:
    """Run a single test tier using connections from the persistent pool."""
    queries_per_client = total_requests // clients
    remainder = total_requests % clients
    assignments = []
    for i in range(clients):
        count = queries_per_client + (1 if i < remainder else 0)
        assignments.append([random.choice(id_pool) for _ in range(count)])

    tier_conns = pool[:clients]

    all_results = []
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=clients) as executor:
        futures = []
        for i in range(clients):
            futures.append(executor.submit(worker, tier_conns[i], assignments[i]))
        for future in as_completed(futures):
            all_results.extend(future.result())
    elapsed = time.perf_counter() - start

    return all_results, elapsed


def get_server_side_metrics(conn) -> list[float]:
    """Fetch server-side execution times from ACCOUNT_USAGE."""
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    cur.execute("""
        SELECT query_id, total_elapsed_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE warehouse_name = 'COMPUTE_WH'
          AND start_time >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
          AND query_text ILIKE '%MDP_PLC_ST_HYBRID%'
          AND query_text ILIKE '%CONSENSUS_UNIQUE_IDENTIFIER%'
          AND query_type = 'SELECT'
          AND query_text NOT ILIKE '%ACCOUNT_USAGE%'
          AND query_text NOT ILIKE '%SAMPLE%'
        ORDER BY start_time DESC
        LIMIT 2000
    """)
    rows = cur.fetchall()
    cur.close()
    server_times = [row[1] for row in rows if row[1] is not None and row[1] > 0]
    return server_times


def compute_percentiles(latencies: list[float]) -> dict:
    """Compute p50, p95, p99, max from a list of latencies in ms."""
    if not latencies:
        return {"p50": 0, "p95": 0, "p99": 0, "max": 0}
    arr = np.array(latencies)
    return {
        "p50": int(np.percentile(arr, 50)),
        "p95": int(np.percentile(arr, 95)),
        "p99": int(np.percentile(arr, 99)),
        "max": int(np.max(arr)),
    }


def print_header():
    log("=" * 90)
    log("MDP_PLC_ST_HYBRID Point Lookup Stress Test (Hybrid Table - B-tree PK)")
    log(f"Warehouse: COMPUTE_WH (Standard, on-demand)")
    log(f"Table: {TABLE} (20M rows, hybrid)")
    log(f"Query: SELECT * WHERE CONSENSUS_UNIQUE_IDENTIFIER = <value>")
    log(f"Auth: PAT (programmatic access token)")
    log(f"Pool: {MAX_POOL_SIZE} persistent connections (opened once, reused)")
    log("=" * 90)
    log("")


def print_results(tier_results: list[dict]):
    log("CLIENT-SIDE LATENCY (includes network round trip, excludes connection setup):")
    log(f"{'Clients':>8} {'Requests':>9} {'Success':>9} {'Throughput':>12} "
        f"{'p50(ms)':>8} {'p95(ms)':>8} {'p99(ms)':>8} {'Max(ms)':>8}")
    log("-" * 90)
    for r in tier_results:
        log(f"{r['clients']:>8} {r['requests']:>9} "
            f"{r['success']}/{r['requests']:>5} "
            f"{r['throughput']:>9.1f} req/s "
            f"{r['client_p50']:>8} {r['client_p95']:>8} "
            f"{r['client_p99']:>8} {r['client_max']:>8}")
    log("")

    log("SERVER-SIDE LATENCY (Snowflake execution time from ACCOUNT_USAGE):")
    log(f"{'Clients':>8} {'Requests':>9} "
        f"{'p50(ms)':>8} {'p95(ms)':>8} {'p99(ms)':>8} {'Max(ms)':>8}")
    log("-" * 60)
    for r in tier_results:
        if r.get("server_p50") is not None:
            log(f"{r['clients']:>8} {r['requests']:>9} "
                f"{r['server_p50']:>8} {r['server_p95']:>8} "
                f"{r['server_p99']:>8} {r['server_max']:>8}")
        else:
            log(f"{r['clients']:>8} {r['requests']:>9} "
                f"{'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8}")
    log("")


def main():
    print_header()

    # Step 1: Open persistent connection pool (one-time cost)
    log("Phase 1: Setting up persistent connection pool...")
    pool_start = time.perf_counter()
    pool = open_connection_pool(MAX_POOL_SIZE)
    pool_time = time.perf_counter() - pool_start
    log(f"  Pool ready: {len(pool)} connections in {pool_time:.1f}s (one-time cost)\n")

    # Step 2: Fetch ID pool using first connection
    log("Phase 2: Fetching lookup IDs...")
    id_pool = fetch_id_pool(pool[0], ID_POOL_SIZE)
    log(f"  Fetched {len(id_pool)} valid IDs\n")

    # Step 3: Warm-up (using persistent pool)
    log(f"Phase 3: Warm-up ({WARMUP_QUERIES} queries on pool[0])...")
    for _ in range(WARMUP_QUERIES):
        run_single_query(pool[0], random.choice(id_pool))
    log("  Warm-up complete.\n")

    # Step 4: Run test tiers
    log("Phase 4: Running stress test tiers...")
    log("")
    tier_results = []
    for tier in TEST_TIERS:
        clients = tier["clients"]
        total_requests = tier["requests"]
        log(f"  Tier: {clients} clients x {total_requests} requests...")

        results, elapsed = run_tier(pool, clients, total_requests, id_pool)

        successes = sum(1 for r in results if r.success)
        latencies = [r.client_latency_ms for r in results if r.success]
        throughput = len(results) / elapsed if elapsed > 0 else 0
        pcts = compute_percentiles(latencies)

        tier_result = {
            "clients": clients,
            "requests": total_requests,
            "success": successes,
            "throughput": throughput,
            "client_p50": pcts["p50"],
            "client_p95": pcts["p95"],
            "client_p99": pcts["p99"],
            "client_max": pcts["max"],
        }
        log(f"    -> {successes}/{total_requests} success | "
            f"p50={pcts['p50']}ms | p99={pcts['p99']}ms | "
            f"{throughput:.1f} req/s | {elapsed:.2f}s")

        tier_results.append(tier_result)

    log("")

    # Step 5: Fetch server-side metrics
    log("Phase 5: Server-side metrics from ACCOUNT_USAGE...")
    try:
        server_times = get_server_side_metrics(pool[0])
        if server_times:
            server_pcts = compute_percentiles(server_times)
            log(f"  Found {len(server_times)} server-side query records")
            for r in tier_results:
                r["server_p50"] = server_pcts["p50"]
                r["server_p95"] = server_pcts["p95"]
                r["server_p99"] = server_pcts["p99"]
                r["server_max"] = server_pcts["max"]
        else:
            log("  (Not yet available - ACCOUNT_USAGE has ~45 min latency)")
            for r in tier_results:
                r["server_p50"] = None
    except Exception as e:
        log(f"  (Failed to fetch server metrics: {e})")
        for r in tier_results:
            r["server_p50"] = None

    # Step 6: Close pool
    close_pool(pool)

    # Print final results
    log("\n")
    print_results(tier_results)
    log("=" * 90)
    log("Test complete.")


if __name__ == "__main__":
    main()

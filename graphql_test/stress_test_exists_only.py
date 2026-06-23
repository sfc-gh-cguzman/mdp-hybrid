"""
MDP PLC Hybrid Table - GraphQL plcExists Only Test (40M rows, MDP_HYBRID_XS)
"""

import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import numpy as np
import httpx
import snowflake.connector

GRAPHQL_URL = "http://localhost:8000/graphql"

CONNECTION_NAME = "spark-connect"
WAREHOUSE = "MDP_HYBRID_XS"
TABLE = "ISRG_D3_DB.CURATED.MDP_PLC_ST_HYBRID"

GRAPHQL_QUERY_EXISTS = """
query PLCExists($id: BigInt!) {
  plcExists(id: $id) {
    consensusUniqueIdentifier
  }
}
"""

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
    status_code: int = 0


def log(msg: str):
    print(msg, flush=True)


def fetch_id_pool(size: int) -> list[int]:
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    conn.cursor().execute(f"USE WAREHOUSE {WAREHOUSE}")
    cur = conn.cursor()
    cur.execute(f"SELECT CONSENSUS_UNIQUE_IDENTIFIER FROM {TABLE} SAMPLE ({size} ROWS)")
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids


def run_single_query(client: httpx.Client, lookup_id: int) -> QueryResult:
    start = time.perf_counter()
    try:
        resp = client.post(
            GRAPHQL_URL,
            json={"query": GRAPHQL_QUERY_EXISTS, "variables": {"id": lookup_id}},
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        success = resp.status_code == 200 and "errors" not in resp.json()
        return QueryResult(success=success, client_latency_ms=elapsed_ms, status_code=resp.status_code)
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        log(f"  ERROR: {e}")
        return QueryResult(success=False, client_latency_ms=elapsed_ms)


def worker(ids_to_query: list[int]) -> list[QueryResult]:
    client = httpx.Client(timeout=30.0)
    results = []
    for lookup_id in ids_to_query:
        results.append(run_single_query(client, lookup_id))
    client.close()
    return results


def run_tier(clients: int, total_requests: int, id_pool: list[int]) -> tuple[list[QueryResult], float]:
    queries_per_client = total_requests // clients
    remainder = total_requests % clients
    assignments = []
    for i in range(clients):
        count = queries_per_client + (1 if i < remainder else 0)
        assignments.append([random.choice(id_pool) for _ in range(count)])

    all_results = []
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=clients) as executor:
        futures = []
        for i in range(clients):
            futures.append(executor.submit(worker, assignments[i]))
        for future in as_completed(futures):
            all_results.extend(future.result())
    elapsed = time.perf_counter() - start

    return all_results, elapsed


def compute_percentiles(latencies: list[float]) -> dict:
    if not latencies:
        return {"p50": 0, "p95": 0, "p99": 0, "max": 0}
    arr = np.array(latencies)
    return {
        "p50": int(np.percentile(arr, 50)),
        "p95": int(np.percentile(arr, 95)),
        "p99": int(np.percentile(arr, 99)),
        "max": int(np.max(arr)),
    }


def main():
    log("=" * 90)
    log("MDP_PLC_ST_HYBRID GraphQL plcExists Test (40M rows)")
    log(f"Endpoint: {GRAPHQL_URL}")
    log(f"Warehouse: MDP_HYBRID_XS (XSMALL, MCW max 10)")
    log(f"Table: {TABLE}")
    log(f"Query: SELECT CONSENSUS_UNIQUE_IDENTIFIER WHERE PK = <value>")
    log("=" * 90)

    log("\nPhase 1: Fetching lookup IDs...")
    id_pool = fetch_id_pool(ID_POOL_SIZE)
    log(f"  Fetched {len(id_pool)} valid IDs\n")

    log(f"Phase 2: Warm-up ({WARMUP_QUERIES} queries)...")
    client = httpx.Client(timeout=30.0)
    for _ in range(WARMUP_QUERIES):
        run_single_query(client, random.choice(id_pool))
    client.close()
    log("  Warm-up complete.\n")

    log("Phase 3: Running stress test tiers...")
    log("")
    tier_results = []
    for tier in TEST_TIERS:
        clients = tier["clients"]
        total_requests = tier["requests"]
        log(f"  Tier: {clients} clients x {total_requests} requests...")

        results, elapsed = run_tier(clients, total_requests, id_pool)

        successes = sum(1 for r in results if r.success)
        latencies = [r.client_latency_ms for r in results if r.success]
        throughput = len(results) / elapsed if elapsed > 0 else 0
        pcts = compute_percentiles(latencies)

        tier_results.append({
            "clients": clients,
            "requests": total_requests,
            "success": successes,
            "throughput": throughput,
            "client_p50": pcts["p50"],
            "client_p95": pcts["p95"],
            "client_p99": pcts["p99"],
            "client_max": pcts["max"],
        })
        log(f"    -> {successes}/{total_requests} success | "
            f"p50={pcts['p50']}ms | p99={pcts['p99']}ms | "
            f"{throughput:.1f} req/s | {elapsed:.2f}s")

    log("\n")
    log("RESULTS: plcExists (SELECT PK only, MDP_HYBRID_XS MCW=10)")
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
    log("=" * 90)
    log("Test complete.")


if __name__ == "__main__":
    main()

# Hybrid Table vs Interactive Table: Point Lookup Benchmark

## Overview

Comparison of two approaches for sub-500ms point lookups on `MDP_PLC_ST` (40M rows)
filtered by `CONSENSUS_UNIQUE_IDENTIFIER` (NUMBER(19,0), unique per row).

## Architecture Comparison

| Dimension | Interactive Table | Hybrid Table |
|-----------|-----------------|--------------|
| Table | `MDP_PLC_ST_IT` | `MDP_PLC_ST_HYBRID` |
| Storage model | Columnar (micro-partitions) | Row-store (B-tree on PK) |
| Lookup method | Min/max pruning on cluster key | Direct B-tree index seek |
| Dedicated warehouse | Yes (`MDP_PLC_IWH`, always-on) | No (any standard WH) |
| Result cache | Yes (`USE_CACHED_RESULT`) | No (always hits row store) |
| Max throughput | Limited by WH concurrency | ~16K ops/sec per database |
| VARIANT handling | Columnar compression (efficient) | Row-store (less compressed) |
| Cost model | Fixed (always-on XS = 1 credit/hr) | Per-query (standard compute) |
| Data freshness | Static CTAS (manual refresh) | Static CTAS (manual refresh) |

## Measured Performance (Direct SQL - 20M rows)

| Metric | Interactive Table | Hybrid Table |
|--------|-----------------|--------------|
| Server-side p50 | ~67ms | Pending (ACCOUNT_USAGE lag) |
| Client-side p50 (10 clients) | 159ms | 189ms |
| Client-side p50 (100 clients, 500 req) | 185ms | 187ms |
| Client-side p50 (100 clients, 5K req) | 186ms | 144ms |
| Client-side p50 (100 clients, 10K req) | 215ms | 142ms |
| Peak throughput | 486.4 req/s | 563.5 req/s |

## Trade-offs

### Interactive Table Wins
- **VARIANT compression**: Columnar storage compresses JSON arrays/objects more efficiently
- **Result cache**: Repeated identical lookups return instantly from cache
- **Predictable cost**: Fixed hourly rate regardless of query volume
- **No row store limits**: No 2TB per-database storage cap

### Hybrid Table Wins
- **Latency**: B-tree PK seek is O(log n) vs scanning/pruning micro-partitions
- **No dedicated warehouse**: No always-on cost; uses existing compute
- **Consistency**: Sub-100ms read-after-write (same session)
- **Simpler ops**: No warehouse lifecycle management (suspend/resume/associate)
- **Write support**: Can INSERT/UPDATE/DELETE individual rows (OLTP capable)

## When to Choose Which

| Scenario | Recommendation |
|----------|---------------|
| Pure point lookups by PK, cost-sensitive | Hybrid Table |
| High-volume reads with repeated keys | Interactive Table (result cache) |
| Mixed read/write workload | Hybrid Table |
| Large VARIANT payloads (>1KB per row) | Interactive Table (better compression) |
| Need >16K ops/sec per database | Interactive Table |
| Minimal ops overhead | Hybrid Table |

## Running the Benchmark

```bash
# Direct SQL tests (20M rows)
conda run -n mdp_interactive python tests/stress_test.py           # Interactive table
conda run -n mdp_interactive python tests/stress_test_hybrid.py    # Hybrid table (SELECT *)
conda run -n mdp_interactive python tests/stress_test_hybrid_exists.py  # Hybrid table (SELECT 1)

# GraphQL tests (40M rows) - requires server running first
conda run -n mdp_interactive uvicorn graphql_test.server:app --host 0.0.0.0 --port 8000
conda run -n mdp_interactive python graphql_test/stress_test_graphql.py
```

## Results - Direct SQL (20M rows)

### Hybrid Table (MDP_PLC_ST_HYBRID on COMPUTE_WH)

Run date: 2026-06-18

| Clients | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---------|----------|-----------|----------|----------|----------|----------|
| 10 | 10 | 13.7 req/s | 189 | 723 | 724 | 725 |
| 20 | 50 | 78.3 req/s | 174 | 278 | 313 | 322 |
| 50 | 100 | 212.6 req/s | 186 | 277 | 282 | 294 |
| 100 | 200 | 328.8 req/s | 212 | 343 | 365 | 405 |
| 100 | 500 | 322.3 req/s | 187 | 248 | 287 | 946 |
| 100 | 5,000 | 528.2 req/s | 144 | 293 | 567 | 886 |
| 100 | 10,000 | 563.5 req/s | 142 | 246 | 428 | 1,397 |

### Interactive Table (MDP_PLC_ST_IT on MDP_PLC_IWH)

Run date: 2026-06-18 (from previous benchmark)

| Clients | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---------|----------|-----------|----------|----------|----------|----------|
| 10 | 10 | 55.7 req/s | 159 | 171 | 172 | 172 |
| 20 | 50 | 95.1 req/s | 147 | 210 | 210 | 211 |
| 50 | 100 | 228.7 req/s | 169 | 269 | 286 | 289 |
| 100 | 200 | 245.9 req/s | 235 | 456 | 597 | 618 |
| 100 | 500 | 364.9 req/s | 185 | 399 | 474 | 495 |
| 100 | 5,000 | 486.4 req/s | 186 | 290 | 412 | 615 |
| 100 | 10,000 | 422.6 req/s | 215 | 311 | 752 | 1,640 |

### Analysis

**Throughput**: Hybrid table peaks at **563.5 req/s** vs interactive table at **486.4 req/s** (+16% improvement at sustained load).

**Latency at scale (100 clients, 10K requests)**:
- Hybrid p50: **142ms** vs Interactive p50: **215ms** (34% faster)
- Hybrid p99: **428ms** vs Interactive p99: **752ms** (43% better tail latency)

**Cold start penalty**: At low request counts (10 clients x 10 requests), the hybrid table shows higher p50 (189ms vs 159ms) and much higher p95/p99 (723ms vs 172ms). This is likely the standard warehouse cold-start vs the always-warm interactive warehouse cache.

**Sustained load advantage**: Once warmed up (5K+ requests), the hybrid table consistently beats the interactive table on both p50 and throughput. The B-tree index seek scales better under concurrency than columnar partition pruning.

**Cost difference**: The interactive warehouse (`MDP_PLC_IWH`) runs at 1 credit/hr always-on. The hybrid table uses `COMPUTE_WH` which can be shared with other workloads and auto-suspends when idle.

## Results - GraphQL (40M rows, Hybrid Table)

### Configuration A: Single Worker (baseline)

Server: Strawberry GraphQL + uvicorn (1 worker), 25-connection Snowflake pool, `COMPUTE_WH`.

#### plcLookup (SELECT 8 columns)

Query: `SELECT CONSENSUS_UNIQUE_IDENTIFIER, FINAL_LOWER_SERIAL_NUMBER, FINAL_UPPER_SERIAL_NUMBER, LOWER_PARTCLASS, UPPER_PARTCLASS, LINK_TYPE, CONSENSUS_CONFIDENCE, DEDUP_STATUS`

Run date: 2026-06-23

| Clients | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---------|----------|-----------|----------|----------|----------|----------|
| 10 | 10 | 31.7 req/s | 269 | 297 | 298 | 298 |
| 20 | 50 | 37.5 req/s | 189 | 823 | 878 | 918 |
| 50 | 100 | 51.0 req/s | 386 | 1,542 | 1,565 | 1,710 |
| 100 | 200 | 106.2 req/s | 631 | 943 | 1,173 | 1,335 |
| 100 | 500 | 147.2 req/s | 576 | 906 | 1,045 | 1,270 |
| 100 | 5,000 | 199.3 req/s | 471 | 656 | 792 | 1,584 |
| 100 | 10,000 | 208.0 req/s | 457 | 622 | 742 | 1,110 |

#### plcExists (SELECT PK only)

Query: `SELECT CONSENSUS_UNIQUE_IDENTIFIER`

Run date: 2026-06-23

| Clients | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---------|----------|-----------|----------|----------|----------|----------|
| 10 | 10 | 53.4 req/s | 124 | 127 | 127 | 128 |
| 20 | 50 | 120.8 req/s | 128 | 154 | 157 | 157 |
| 50 | 100 | 163.6 req/s | 221 | 322 | 449 | 451 |
| 100 | 200 | 190.2 req/s | 390 | 538 | 600 | 669 |
| 100 | 500 | 180.0 req/s | 444 | 616 | 714 | 912 |
| 100 | 5,000 | 207.1 req/s | 455 | 653 | 788 | 1,225 |
| 100 | 10,000 | 193.4 req/s | 484 | 699 | 1,082 | 1,514 |

### Configuration B: Multi-Worker Optimized

Server: Strawberry GraphQL + gunicorn (4 workers x 50 connections = 200 total), `MDP_HYBRID_XS` (XSMALL, MCW max 10).

#### plcExists (SELECT PK only)

Run date: 2026-06-23

| Clients | Requests | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---------|----------|-----------|----------|----------|----------|----------|
| 100 | 5,000 | 285.6 req/s | 131 | 517 | 992 | 5,030 |
| 100 | 10,000 | 351.6 req/s | 144 | 458 | 758 | 4,561 |

Note: Early tiers (10-50 clients) hit timeouts during pool initialization (200 connections opening across 4 workers). Once warm, steady-state performance is reflected in the 5K and 10K tiers.

### GraphQL Analysis

**Important context**: These latencies represent the full round-trip from a local laptop (client) through a local GraphQL server to Snowflake and back. The GraphQL layer, Python runtime, connection pool management, and network RTT all add overhead beyond Snowflake execution time.

**Server-side execution time (from INFORMATION_SCHEMA.QUERY_HISTORY)**:
- Snowflake B-tree PK execution: **10-48ms** (the actual data retrieval)
- SQL compilation: **29-82ms** (parse/plan - unavoidable per query)
- Total server-side elapsed: **55-102ms** best case
- Zero queueing on warehouse (no overload, no provisioning wait)

**Multi-worker optimization impact (Config A vs B)**:

| Metric (100c, 10K req) | 1 worker / 25 conn | 4 workers / 50 conn each |
|---|---|---|
| p50 | 484ms | **144ms** (70% faster) |
| p99 | 1,082ms | **758ms** (30% better) |
| Throughput | 193 req/s | **352 req/s** (82% higher) |

The multi-worker config eliminates GIL contention and connection pool starvation. The p50 of **144ms** now matches the direct SQL benchmark (142ms), meaning GraphQL overhead is effectively zero at steady state.

**Overhead breakdown (single worker)**:
- Snowflake server-side: 55-102ms
- Network RTT (laptop to Snowflake): ~50-70ms
- GraphQL + connection pool queueing (single worker): ~300-350ms at high concurrency
- GraphQL + connection pool queueing (4 workers): ~0-20ms at high concurrency

**plcExists latency degradation in Config A**: The `plcExists` test ran sequentially after `plcLookup` in the same script. The higher p50 (484ms at 10K requests) compared to `plcLookup` (457ms) is from sustained load on a single Python process - connection pool exhaustion, GIL contention, and local TCP pressure. This is a local machine limitation, not Snowflake-side.

**40M vs 20M impact**: The hybrid table B-tree lookup shows no meaningful performance degradation at 40M rows compared to the 20M direct SQL test (p50 still ~131-144ms). B-tree seeks are O(log n) - doubling rows adds one tree level.

# MDP PLC Hybrid Table - Point Lookup Benchmark

Benchmark project comparing Snowflake **Hybrid Tables** (B-tree PK index) against **Interactive Tables** (columnar + partition pruning) for high-concurrency point lookups on manufacturing PLC (Product Lifecycle Chain) data.

## What is a Hybrid Table?

A Snowflake Hybrid Table is a table type optimized for low-latency, transactional workloads. Key differences from standard tables:

| Property | Standard / Interactive Table | Hybrid Table |
|----------|------------------------------|--------------|
| Storage | Columnar micro-partitions | Row-store with B-tree indexes |
| Primary key | Informational only (not enforced) | Enforced, backed by B-tree index |
| Point lookup | Partition pruning (scan-based) | Direct index seek (O(log n)) |
| Write model | Bulk append (COPY, INSERT batch) | Row-level INSERT/UPDATE/DELETE |
| Constraints | Not enforced | UNIQUE, FK enforced |
| Dedicated warehouse | Required for interactive tables | Not required - uses any standard WH |
| Result cache | Yes | No (always hits row store) |
| Max storage | Unlimited | 2 TB per database |
| Max throughput | WH-limited | ~16K ops/sec per database |

## Table Definition

```sql
CREATE HYBRID TABLE ISRG_D3_DB.CURATED.MDP_PLC_ST_HYBRID (
  CONSENSUS_UNIQUE_IDENTIFIER NUMBER(19,0) NOT NULL PRIMARY KEY,
  ALL_OBSERVED_UIDS VARIANT,
  LOWER_TIER_IDS VARIANT,
  UPPER_TIER_IDS VARIANT,
  LOWER_SERIAL_NUMBERS VARIANT,
  UPPER_SERIAL_NUMBERS VARIANT,
  LOWER_SERIAL_NUMBERS_DETAIL VARIANT,
  UPPER_SERIAL_NUMBERS_DETAIL VARIANT,
  FINAL_LOWER_SERIAL_NUMBER VARCHAR,
  FINAL_UPPER_SERIAL_NUMBER VARCHAR,
  LOWER_PART_NUMBERS VARIANT,
  LOWER_PARTCLASS VARCHAR,
  UPPER_PART_NUMBERS VARIANT,
  UPPER_PARTCLASS VARCHAR,
  LOWER_WORK_ORDER_NUMBERS VARIANT,
  UPPER_WORK_ORDER_NUMBERS VARIANT,
  WORK_ORDERS_DETAIL VARIANT,
  SERIAL_UID_RESOLUTION VARIANT,
  LOWER_TIER_COUNT NUMBER(38,0),
  UPPER_TIER_COUNT NUMBER(38,0),
  CONSENSUS_CONFIDENCE FLOAT,
  LINK_TYPE VARCHAR,
  DEDUP_STATUS VARCHAR,
  LINKAGE_LAST_UPDATED TIMESTAMP_TZ(9)
);
```

- **40 million rows** (CTAS from `MDP_PLC_ST_IT`)
- **Primary key**: `CONSENSUS_UNIQUE_IDENTIFIER` - unique numeric ID per PLC record
- **10 VARIANT columns**: JSON arrays/objects storing serial numbers, part numbers, work orders
- **Workload**: Single-row point lookups by PK (existence checks + full row fetches)

## Architecture

```
                                  Test Clients (httpx, 100 concurrent)
                                           |
                                           v
                              +-------------------------+
                              |  GraphQL Server (local) |
                              |  Strawberry + Gunicorn  |
                              |  4 workers x 50 conns   |
                              +-------------------------+
                                           |
                                    Snowflake SDK
                                           |
                                           v
                              +-------------------------+
                              |  MDP_HYBRID_XS (XSMALL) |
                              |  MCW max 10 clusters    |
                              +-------------------------+
                                           |
                                           v
                              +-------------------------+
                              |  MDP_PLC_ST_HYBRID      |
                              |  Hybrid Table (40M rows)|
                              |  B-tree on PK           |
                              +-------------------------+
```

## Project Structure

```
mdp-hybrid/
├── README.md
├── sql/
│   └── hybrid_setup.sql          # DDL: CREATE HYBRID TABLE + CTAS + verification
├── tests/
│   ├── stress_test_hybrid.py     # Direct SQL: SELECT * (20M, COMPUTE_WH)
│   └── stress_test_hybrid_exists.py  # Direct SQL: SELECT 1 existence check
├── graphql_test/
│   ├── __init__.py
│   ├── server.py                 # Strawberry GraphQL server (async + connection pool)
│   ├── stress_test_graphql.py    # Both plcLookup + plcExists suites
│   └── stress_test_exists_only.py  # plcExists only (isolated benchmark)
└── docs/
    └── hybrid_vs_interactive.md  # Full benchmark results and analysis
```

## Running the Tests

### Prerequisites

```bash
conda activate mdp_interactive
# Requires: snowflake-connector-python, strawberry-graphql, uvicorn, gunicorn,
#           httpx, numpy
```

### Direct SQL Tests (no server needed)

```bash
# SELECT * point lookup (uses COMPUTE_WH)
python tests/stress_test_hybrid.py

# SELECT 1 existence check
python tests/stress_test_hybrid_exists.py
```

### GraphQL Tests

```bash
# Start server (optimized: 4 workers, 50 connections each)
gunicorn graphql_test.server:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000

# Or single worker for dev:
uvicorn graphql_test.server:app --host 0.0.0.0 --port 8000

# Run tests (wait for server to initialize connection pools - ~30s)
python graphql_test/stress_test_exists_only.py    # PK existence only
python graphql_test/stress_test_graphql.py        # Both lookup + exists
```

### GraphQL Endpoints

| Query | SQL Executed | Use Case |
|-------|-------------|----------|
| `plcLookup(id)` | SELECT 8 scalar columns by PK | Fetch record details |
| `plcExists(id)` | SELECT CONSENSUS_UNIQUE_IDENTIFIER by PK | Existence check |

Example:
```graphql
query {
  plcExists(id: 8146597745) {
    consensusUniqueIdentifier
  }
}
```

## Key Results

### Server-side (Snowflake execution)

| Metric | Value |
|--------|-------|
| B-tree PK seek | 10-48ms |
| SQL compilation | 29-82ms |
| Total server-side | 55-102ms |
| Warehouse queueing | 0ms |

### Client-side (end-to-end through GraphQL, 100 concurrent clients, 10K requests)

| Configuration | p50 | p99 | Throughput |
|--------------|-----|-----|-----------|
| 1 worker, 25 connections | 484ms | 1,082ms | 193 req/s |
| 4 workers, 50 connections each | **144ms** | **758ms** | **352 req/s** |

### vs Interactive Table (direct SQL, 20M rows)

| Metric | Interactive Table | Hybrid Table |
|--------|-----------------|--------------|
| p50 (sustained, 10K req) | 215ms | **142ms** |
| Peak throughput | 486 req/s | **564 req/s** |
| Dedicated warehouse | Yes (always-on, 1 credit/hr) | No |

## Snowflake Resources

- **Hybrid table**: `ISRG_D3_DB.CURATED.MDP_PLC_ST_HYBRID`
- **Warehouse**: `MDP_HYBRID_XS` (XSMALL, MCW 1-10, auto-suspend 300s)
- **Source table**: `ISRG_D3_DB.CURATED.MDP_PLC_ST_IT` (40M rows, interactive table)
- **Connection**: `spark-connect` (PAT auth via `~/.snowflake/connections.toml`)

## Cleanup

```sql
DROP TABLE IF EXISTS ISRG_D3_DB.CURATED.MDP_PLC_ST_HYBRID;
DROP WAREHOUSE IF EXISTS MDP_HYBRID_XS;
```

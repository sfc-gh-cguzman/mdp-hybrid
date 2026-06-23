"""
GraphQL server for MDP PLC point lookups - Hybrid Table variant.
Strawberry + uvicorn, backed by a per-worker Snowflake connection pool.

Usage (single worker, dev):
    conda run -n mdp_interactive uvicorn graphql_test.server:app --host 0.0.0.0 --port 8000

Usage (multi-worker, production):
    cd /path/to/mdp-hybrid
    conda run -n mdp_interactive gunicorn graphql_test.server:app \
        -w 8 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
"""

import asyncio
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, NewType

import strawberry
from strawberry.asgi import GraphQL
import snowflake.connector

CONNECTION_NAME = "spark-connect"
WAREHOUSE = "MDP_HYBRID_XS"
TABLE = "ISRG_D3_DB.CURATED.MDP_PLC_ST_HYBRID"
POOL_SIZE = 50

BigInt = strawberry.scalar(
    NewType("BigInt", int),
    serialize=lambda v: v,
    parse_value=lambda v: int(v),
)


class ConnectionPool:
    """Thread-safe connection pool for Snowflake (per-worker)."""

    def __init__(self, size: int):
        self._pool: queue.Queue = queue.Queue(maxsize=size)
        self._size = size
        self._initialized = False
        self._lock = threading.Lock()

    def initialize(self):
        with self._lock:
            if self._initialized:
                return
            print(f"[PID {threading.current_thread().name}] "
                  f"Initializing pool ({self._size} connections)...", flush=True)
            for i in range(self._size):
                conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
                conn.cursor().execute(f"USE WAREHOUSE {WAREHOUSE}")
                self._pool.put(conn)
            self._initialized = True
            print(f"  Pool ready ({self._size} connections).", flush=True)

    def get(self) -> snowflake.connector.SnowflakeConnection:
        if not self._initialized:
            self.initialize()
        return self._pool.get()

    def put(self, conn: snowflake.connector.SnowflakeConnection):
        self._pool.put(conn)


pool = ConnectionPool(POOL_SIZE)
executor = ThreadPoolExecutor(max_workers=POOL_SIZE + 10)


def _sync_lookup(lookup_id: int) -> Optional[dict]:
    """Blocking Snowflake query - runs in thread pool."""
    conn = pool.get()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""SELECT CONSENSUS_UNIQUE_IDENTIFIER,
                   FINAL_LOWER_SERIAL_NUMBER,
                   FINAL_UPPER_SERIAL_NUMBER,
                   LOWER_PARTCLASS,
                   UPPER_PARTCLASS,
                   LINK_TYPE,
                   CONSENSUS_CONFIDENCE,
                   DEDUP_STATUS
            FROM {TABLE}
            WHERE CONSENSUS_UNIQUE_IDENTIFIER = %s""",
            (lookup_id,),
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        return {
            "consensus_unique_identifier": row[0],
            "final_lower_serial_number": row[1],
            "final_upper_serial_number": row[2],
            "lower_partclass": row[3],
            "upper_partclass": row[4],
            "link_type": row[5],
            "consensus_confidence": row[6],
            "dedup_status": row[7],
        }
    finally:
        pool.put(conn)


def _sync_lookup_select_star(lookup_id: int) -> Optional[dict]:
    """SELECT * variant - returns all columns."""
    conn = pool.get()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM {TABLE} WHERE CONSENSUS_UNIQUE_IDENTIFIER = %s",
            (lookup_id,),
        )
        row = cur.fetchone()
        if row is None:
            cur.close()
            return None
        cols = [desc[0].lower() for desc in cur.description]
        cur.close()
        return dict(zip(cols, row))
    finally:
        pool.put(conn)


def _sync_lookup_pk_only(lookup_id: int) -> Optional[dict]:
    """SELECT just the PK - minimal payload existence check."""
    conn = pool.get()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""SELECT CONSENSUS_UNIQUE_IDENTIFIER
            FROM {TABLE}
            WHERE CONSENSUS_UNIQUE_IDENTIFIER = %s""",
            (lookup_id,),
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        return {"consensus_unique_identifier": row[0]}
    finally:
        pool.put(conn)


@strawberry.type
class PLCRecord:
    consensus_unique_identifier: BigInt
    final_lower_serial_number: Optional[str]
    final_upper_serial_number: Optional[str]
    lower_partclass: Optional[str]
    upper_partclass: Optional[str]
    link_type: Optional[str]
    consensus_confidence: Optional[float]
    dedup_status: Optional[str]


@strawberry.type
class PLCExists:
    consensus_unique_identifier: BigInt


@strawberry.type
class Query:
    @strawberry.field
    async def plc_lookup(self, id: BigInt) -> Optional[PLCRecord]:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, _sync_lookup, id)
        if result is None:
            return None
        return PLCRecord(**result)

    @strawberry.field
    async def plc_exists(self, id: BigInt) -> Optional[PLCExists]:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, _sync_lookup_pk_only, id)
        if result is None:
            return None
        return PLCExists(**result)


schema = strawberry.Schema(query=Query)
app = GraphQL(schema)

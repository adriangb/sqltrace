import asyncio
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from sqltrace.psycopg import async_setup_auto_explain, AsyncCursor, notice_handler

provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer('test')


async def main() -> None:
    dsn = "postgresql://postgres:postgres@localhost:5432/postgres"
    async def configure(conn: AsyncConnection[Any]) -> None:
        conn.add_notice_handler(notice_handler)

    async with AsyncConnectionPool(dsn, configure=configure, kwargs={'autocommit': True}) as pool:
        async with pool.connection() as conn:
            await async_setup_auto_explain(conn, 10)

        with tracer.start_as_current_span('test'):
            async with pool.connection() as conn:
                conn.cursor_factory = AsyncCursor
                # not logged
                await conn.execute("SELECT 1")
                # logged
                await conn.execute("SELECT pg_sleep(1)")


asyncio.run(main())


# import psycopg


# dsn = "postgresql://postgres:postgres@localhost:5432/postgres"
# conn = psycopg.connect(dsn, autocommit=True)
# conn.add_notice_handler(log_notice)

# cur = conn.execute("ROLLBACK")
# print(cur.statusmessage)

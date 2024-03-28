from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from psycopg import Connection
from psycopg_pool import ConnectionPool
from sqltrace.psycopg import setup_auto_explain, Cursor, notice_handler

provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("test")


dsn = "postgresql://postgres:postgres@localhost:5432/postgres"


def configure(conn: Connection[Any]) -> None:
    conn.add_notice_handler(notice_handler)


with ConnectionPool(dsn, configure=configure, kwargs={"autocommit": True}) as pool:
    with pool.connection() as conn:
        setup_auto_explain(conn, 10)

    with tracer.start_as_current_span("test"):
        with pool.connection() as conn:
            conn.cursor_factory = Cursor
            # not logged
            conn.execute("SELECT 1")
            # logged
            conn.execute("SELECT pg_sleep(1)")

from __future__ import annotations

from typing import Any, Iterable, LiteralString, Mapping, Self, Sequence, cast



try:
    import psycopg
    from psycopg import sql
    from psycopg.abc import Query
    from psycopg.rows import Row
except ImportError:
    raise ImportError("You must have psycopg installed to use sqltrace.psycopg")

from sqltrace._propagation import Diagnostic, auto_explain_notice_handler, add_sql_comment


async def async_setup_auto_explain(
    conn: psycopg.AsyncConnection[Any], min_duration_ms: int
) -> None:
    """Configure auto_explain for use with sqltrace.

    You'll only be able to use this if you have permissions to alter the database configuration,
    which you may not have in a managed database service.

    In that case you will have to set these parameters from their cloud console and add
    the notice handler to the connection.
    """
    autocommit = conn.autocommit
    await conn.set_autocommit(True)
    await conn.execute("LOAD 'auto_explain';")
    await conn.execute("SELECT pg_reload_conf();")
    await conn.execute("ALTER SYSTEM SET session_preload_libraries = auto_explain;")
    await conn.execute(
        sql.SQL(
            "ALTER SYSTEM SET auto_explain.log_min_duration = {min_duration_ms};"
        ).format(min_duration_ms=min_duration_ms)
    )
    await conn.execute("ALTER SYSTEM SET auto_explain.log_analyze = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_buffers = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_timing = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_triggers = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_nested_statements = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_settings = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_wal = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_verbose = true;")
    await conn.execute("ALTER SYSTEM SET auto_explain.sample_rate = 1;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_format = JSON;")
    await conn.execute("ALTER SYSTEM SET auto_explain.log_level = notice;")
    await conn.execute("SELECT pg_reload_conf();")
    conn.add_notice_handler(notice_handler)
    await conn.set_autocommit(autocommit)


def setup_auto_explain(
    conn: psycopg.Connection[Any], min_duration_ms: int
) -> None:
    """Configure auto_explain for use with sqltrace.

    You'll only be able to use this if you have permissions to alter the database configuration,
    which you may not have in a managed database service.

    In that case you will have to set these parameters from their cloud console and add
    the notice handler to the connection.
    """
    autocommit = conn.autocommit
    conn.autocommit = True
    conn.execute("LOAD 'auto_explain';")
    conn.execute("SELECT pg_reload_conf();")
    conn.execute("ALTER SYSTEM SET session_preload_libraries = auto_explain;")
    conn.execute(
        sql.SQL(
            "ALTER SYSTEM SET auto_explain.log_min_duration = {min_duration_ms};"
        ).format(min_duration_ms=min_duration_ms)
    )
    conn.execute("ALTER SYSTEM SET auto_explain.log_analyze = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_buffers = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_timing = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_triggers = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_nested_statements = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_settings = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_wal = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_verbose = true;")
    conn.execute("ALTER SYSTEM SET auto_explain.sample_rate = 1;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_format = JSON;")
    conn.execute("ALTER SYSTEM SET auto_explain.log_level = notice;")
    conn.execute("SELECT pg_reload_conf();")
    conn.add_notice_handler(notice_handler)
    conn.autocommit = autocommit


def notice_handler(notice: psycopg.errors.Diagnostic) -> None:
    auto_explain_notice_handler(
        Diagnostic(
            source_file=notice.source_file,
            message_primary=notice.message_primary,
        )
    )


def _insert_context(command: Query, cur: psycopg.AsyncCursor[Any] | psycopg.Cursor[Any]) -> Query:
    q: LiteralString | bytes
    if isinstance(command, sql.Composable):
        q = cast(LiteralString, command.as_string(cur))
    else:
        q = command
    return add_sql_comment(q)


class Cursor(psycopg.Cursor[Row]):
    def execute(self, query: Query, params: Sequence[Any] | Mapping[str, Any] | None = None, *, prepare: bool | None = None, binary: bool | None = None) -> Self:
        query = _insert_context(query, self)
        return super().execute(query, params, prepare=prepare, binary=binary)
    
    def executemany(self, query: Query, params_seq: Iterable[Sequence[Any] | Mapping[str, Any]], *, returning: bool = False) -> None:
        query = _insert_context(query, self)
        return super().executemany(query, params_seq, returning=returning)



class AsyncCursor(psycopg.AsyncCursor[Row]):
    async def execute(self, query: Query, params: Sequence[Any] | Mapping[str, Any] | None = None, *, prepare: bool | None = None, binary: bool | None = None) -> Self:
        query = _insert_context(query, self)
        return await super().execute(query, params, prepare=prepare, binary=binary)
    
    async def executemany(self, query: Query, params_seq: Iterable[Sequence[Any] | Mapping[str, Any]], *, returning: bool = False) -> None:
        query = _insert_context(query, self)
        return await super().executemany(query, params_seq, returning=returning)

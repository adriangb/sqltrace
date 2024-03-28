"""Partially vendored from https://github.com/google/sqlcommenter/blob/master/python/sqlcommenter-python/google/cloud/sqlcommenter/__init__.py"""


from __future__ import annotations

from dataclasses import dataclass
import json
from datetime import datetime, timezone
from typing import Any, Protocol, Sequence, overload
from urllib.parse import unquote_plus

from opentelemetry import context, propagate, trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator  # type: ignore

from typing import LiteralString
from urllib.parse import quote_plus


tracer = trace.get_tracer('sqltrace')

propagator = TraceContextTextMapPropagator()


KEY_VALUE_DELIMITER = ','


def _generate_sql_comment(meta: dict[str, str]) -> str:
    """
    Return a SQL comment with comma delimited key=value pairs created from
    **meta kwargs.
    """
    if not meta:  # No entries added.
        return ''

    # Sort the keywords to ensure that caching works and that testing is
    # deterministic. It eases visual inspection as well.

    return (
        ' /*'
        + KEY_VALUE_DELIMITER.join(f'{_url_quote(key)}={_url_quote(value)}' for key, value in sorted(meta.items()))
        + '*/'
    )



@overload
def add_sql_comment(sql: bytes) -> bytes:
    ...


@overload
def add_sql_comment(sql: LiteralString) -> LiteralString:
    ...


@overload
def add_sql_comment(sql: str) -> str:
    ...


def add_sql_comment(sql: str | bytes) -> str | bytes:
    if not trace.get_current_span().is_recording():
        return sql
    comment = _generate_sql_comment(_get_opentelemetry_values())
    new = sql.rstrip()
    if isinstance(new, bytes):
        comment = comment.encode('utf-8')
        if new[-1:] == b';':
            new = new[:-1] + comment + b';'
        else:
            new = new + comment
        return new
    else:
        if new[-1:] == ';':
            new = new[:-1] + comment + ';'
        else:
            new = new + comment
        return new


def _get_opentelemetry_values() -> dict[str, str]:
    """Return the OpenTelemetry Trace and Span IDs if Span ID is set in the OpenTelemetry execution context."""
    # Insert the W3C TraceContext generated
    headers: dict[str, str] = {}
    propagator.inject(headers)
    return headers

def _url_quote(s: str) -> str:
    quoted = quote_plus(s)
    # Since SQL uses '%' as a keyword, '%' is a by-product of url quoting
    # e.g. foo,bar --> foo%2Cbar
    # thus in our quoting, we need to escape it too to finally give
    #      foo,bar --> foo%%2Cbar
    return quoted.replace('%', '%%')


@dataclass
class Diagnostic:
    source_file: str | None
    message_primary: str | None


_JsonPlan = dict[str, Any]
_Attributes = dict[str, Any]

class _Hook(Protocol):
    def __call__(self, plan: _JsonPlan, duration: float, query: str, attributes: _Attributes) -> _Attributes:
        ...


def auto_explain_notice_handler(diagnostic: Diagnostic, hooks: Sequence[_Hook] = ()) -> None:
    try:
        if diagnostic.source_file == 'auto_explain.c':
            assert diagnostic.message_primary is not None
            duration = float(diagnostic.message_primary.removeprefix('duration: ').split(' ', maxsplit=1)[0])
            plan = json.loads(diagnostic.message_primary.split('plan:\n', maxsplit=1)[1])
            query = str(plan['Query Text'])
            # parse out SQLCommenter style tracing information
            # see https://google.github.io/sqlcommenter/spec/#format

            # this is just a guess at the format, if it fails we'll catch the exception and return
            # (it doesn't have to be perfect)
            if 'traceparent=' in query:
                comment_start = query.rindex('/*')
                comment = query[comment_start + 2 : -2].strip()
                parts = comment.split(',')
                parsed_qs: dict[str, str] = {}
                for part in parts:
                    key, value = part.split('=', maxsplit=1)
                    key = unquote_plus(key.strip().replace('%%', '%'))
                    value = unquote_plus(value.strip().replace('%%', '%'))
                    parsed_qs[key] = value
                
                ctx = propagate.extract(carrier=parsed_qs)
                # convert the start time to a nano timestamp as expected by opentelemetry
                start_time = int(float(parsed_qs['start_time'][0]) * 1e9)
                query = query[:comment_start].strip() + ';'
                attributes = {
                    'db.statement': query,
                    'db.plan': json.dumps(plan['Plan']),
                    **parsed_qs,
                }
                for _hook in hooks:
                    attributes = _hook(plan, duration, query, attributes)
                token = context.attach(ctx)
                try:
                    span = tracer.start_span(
                        name='query-plan',
                        kind=trace.SpanKind.INTERNAL,
                        attributes=attributes,
                        start_time=start_time,
                    )
                    # note that this duration is not "real"
                    # there's things happening before and after the query that we're not capturing
                    # e.g. transferring data / parameters over the wire
                    # but it's the best we can do!
                    span.end(end_time=start_time + int(duration * 1e6))  # duration from auto_explain is in ms
                finally:
                    context.detach(token)
    except Exception:
        return

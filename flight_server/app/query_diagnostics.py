"""
Static SQL query analysis, for diagnostics only.

This is what remains of the pre-ADR-0001 `query_orchestrator.py`: the heuristics that
scored a query's complexity to decide DuckDB vs Trino routing. That routing decision no
longer exists — HybridBackend routes all writes to Iceberg, and DuckDB is a read-only
view layer over it (see docs/adr/0001-canonical-hybrid-backend.md). What's kept here is
purely informational: a quick way to eyeball how "heavy" a query looks (table count,
joins, aggregations, subqueries) when debugging or planning dbt models. It does not
execute anything and does not decide where a query runs.
"""
import re
from dataclasses import dataclass
from enum import Enum


class QueryType(Enum):
    """Rough shape of a query, for diagnostic display only."""
    ANALYTICAL = "analytical"
    OPERATIONAL = "operational"
    AGGREGATION = "aggregation"
    SIMPLE_SELECT = "simple_select"
    JOIN = "join"
    COMPLEX = "complex"


@dataclass
class QueryMetrics:
    """Metrics describing a query's shape. Informational only — not used for routing."""
    table_count: int = 0
    has_joins: bool = False
    has_aggregations: bool = False
    has_subqueries: bool = False
    complexity_score: float = 0.0
    query_type: QueryType = QueryType.SIMPLE_SELECT


def analyze_query(query: str) -> QueryMetrics:
    """Analyze a SQL query's textual shape and return a QueryMetrics summary."""
    query_lower = query.lower()
    metrics = QueryMetrics()

    # Extract table names
    table_pattern = r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)|join\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    tables = re.findall(table_pattern, query_lower)
    table_names = [t[0] or t[1] for t in tables]
    metrics.table_count = len(set(table_names))

    # Check for joins
    metrics.has_joins = bool(re.search(r'\bjoin\b', query_lower))

    # Check for aggregations
    aggregation_keywords = ['group by', 'having', 'count(', 'sum(', 'avg(', 'max(', 'min(']
    metrics.has_aggregations = any(keyword in query_lower for keyword in aggregation_keywords)

    # Check for subqueries: an opening paren directly followed by SELECT,
    # not just any parenthesized expression (e.g. COUNT(*) is not a subquery)
    metrics.has_subqueries = bool(re.search(r'\(\s*select\b', query_lower))

    # Calculate complexity score
    complexity_score = 0.0
    complexity_score += metrics.table_count * 1.0
    complexity_score += 2.0 if metrics.has_joins else 0.0
    complexity_score += 1.5 if metrics.has_aggregations else 0.0
    complexity_score += 2.0 if metrics.has_subqueries else 0.0

    if 'window' in query_lower or 'over(' in query_lower:
        complexity_score += 2.0
    if 'recursive' in query_lower or 'with' in query_lower:
        complexity_score += 1.5

    metrics.complexity_score = complexity_score

    # Determine query type
    if metrics.has_aggregations and metrics.table_count > 1:
        metrics.query_type = QueryType.ANALYTICAL
    elif metrics.has_joins:
        metrics.query_type = QueryType.JOIN
    elif metrics.has_aggregations:
        metrics.query_type = QueryType.AGGREGATION
    elif metrics.complexity_score > 3.0:
        metrics.query_type = QueryType.COMPLEX
    else:
        metrics.query_type = QueryType.SIMPLE_SELECT

    return metrics

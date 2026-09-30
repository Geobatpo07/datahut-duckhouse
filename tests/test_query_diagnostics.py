"""
Tests for query_diagnostics (static query analysis only — see ADR-0001).

This replaces test_query_orchestrator.py. The routing/execution behavior it used to
test (DuckDB vs Trino selection, table registry, data-size estimation) was removed
along with query_orchestrator.py: HybridBackend now routes all writes to Iceberg
unconditionally, so there is no routing decision left to test. What remains is purely
the textual complexity analysis, kept as a diagnostic utility.
"""

from flight_server.app.query_diagnostics import (
    QueryMetrics,
    QueryType,
    analyze_query,
)


class TestQueryMetrics:
    """Test QueryMetrics dataclass."""

    def test_default_values(self):
        """Test default values."""
        metrics = QueryMetrics()
        assert metrics.table_count == 0
        assert metrics.has_joins is False
        assert metrics.has_aggregations is False
        assert metrics.has_subqueries is False
        assert metrics.complexity_score == 0.0
        assert metrics.query_type == QueryType.SIMPLE_SELECT


class TestAnalyzeQuery:
    """Test analyze_query's static analysis of SQL text."""

    def test_analyze_query_simple(self):
        """Test query analysis for simple queries."""
        query = "SELECT * FROM local_patients"
        metrics = analyze_query(query)

        assert metrics.table_count == 1
        assert metrics.has_joins is False
        assert metrics.has_aggregations is False
        assert metrics.has_subqueries is False
        assert metrics.query_type == QueryType.SIMPLE_SELECT
        assert metrics.complexity_score == 1.0  # 1 table * 1.0

    def test_analyze_query_with_joins(self):
        """Test query analysis with joins."""
        query = "SELECT * FROM patients p JOIN diseases d ON p.disease_id = d.id"
        metrics = analyze_query(query)

        assert metrics.table_count == 2
        assert metrics.has_joins is True
        assert metrics.has_aggregations is False
        assert metrics.query_type == QueryType.JOIN
        assert metrics.complexity_score == 4.0  # 2 tables + 2 for joins

    def test_analyze_query_with_aggregations(self):
        """Test query analysis with aggregations."""
        query = "SELECT disease, COUNT(*) FROM patients GROUP BY disease"
        metrics = analyze_query(query)

        assert metrics.table_count == 1
        assert metrics.has_joins is False
        assert metrics.has_aggregations is True
        assert metrics.query_type == QueryType.AGGREGATION
        assert metrics.complexity_score == 2.5  # 1 table + 1.5 for aggregations

    def test_analyze_query_analytical(self):
        """Test query analysis for analytical queries."""
        query = (
            "SELECT disease, COUNT(*), AVG(age) FROM patients p "
            "JOIN outcomes o ON p.id = o.patient_id GROUP BY disease"
        )
        metrics = analyze_query(query)

        assert metrics.table_count == 2
        assert metrics.has_joins is True
        assert metrics.has_aggregations is True
        assert metrics.query_type == QueryType.ANALYTICAL
        assert metrics.complexity_score == 5.5  # 2 tables + 2 joins + 1.5 aggregations

    def test_analyze_query_complex(self):
        """Test query analysis for complex queries."""
        query = """
        SELECT disease,
               COUNT(*) OVER (PARTITION BY disease) as disease_count,
               AVG(age) OVER (PARTITION BY gender) as avg_age_by_gender
        FROM patients
        WHERE id IN (SELECT patient_id FROM outcomes WHERE result = 'positive')
        """
        metrics = analyze_query(query)

        assert metrics.has_subqueries is True
        assert metrics.complexity_score > 3.0  # Should be classified as complex

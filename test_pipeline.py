"""Tests for Snowflake pipeline SQL generation.

These tests verify the SQL output of the query builders and pipeline
without requiring a live Snowflake connection.
"""

from query_builder import MergeBuilder, CopyIntoBuilder, GrantBuilder
from pipeline import StagingPipeline


# ---------------------------------------------------------------------------
# MergeBuilder tests
# ---------------------------------------------------------------------------

def test_merge_excludes_join_keys_from_update():
    """MERGE UPDATE SET should not include join key columns.

    Snowflake raises an error if you try to update columns used in the
    ON clause. The builder must exclude join keys from the SET clause.
    """
    builder = MergeBuilder(
        target_table="PROD.CUSTOMERS",
        source_table="STAGING.CUSTOMERS",
        join_keys=["CUSTOMER_ID"],
        columns=["CUSTOMER_ID", "NAME", "EMAIL", "UPDATED_AT"],
    )
    sql = builder.build()

    # The UPDATE SET clause must NOT contain the join key
    update_line = [l for l in sql.splitlines() if "UPDATE SET" in l][0]
    assert "CUSTOMER_ID" not in update_line, (
        f"Join key 'CUSTOMER_ID' should be excluded from UPDATE SET, "
        f"but found it in: {update_line}"
    )


def test_merge_includes_all_columns_in_insert():
    """MERGE INSERT should include all columns including join keys."""
    builder = MergeBuilder(
        target_table="PROD.ORDERS",
        source_table="STAGING.ORDERS",
        join_keys=["ORDER_ID"],
        columns=["ORDER_ID", "CUSTOMER_ID", "AMOUNT", "ORDER_DATE"],
    )
    sql = builder.build()

    insert_line = [l for l in sql.splitlines() if "INSERT" in l][0]
    for col in ["ORDER_ID", "CUSTOMER_ID", "AMOUNT", "ORDER_DATE"]:
        assert col in insert_line, f"Column '{col}' should be in INSERT clause"


# ---------------------------------------------------------------------------
# CopyIntoBuilder tests
# ---------------------------------------------------------------------------

def test_copy_into_quotes_stage_with_spaces():
    """Stage paths with spaces must be single-quoted in the SQL."""
    builder = CopyIntoBuilder(
        table="RAW.EVENTS",
        stage="@MY_STAGE/path with spaces/data",
        file_format="JSON",
    )
    sql = builder.build()

    # The FROM line should have the stage path wrapped in single quotes
    from_line = [l for l in sql.splitlines() if l.startswith("FROM")][0]
    assert "'" in from_line, (
        f"Stage path with spaces should be quoted on the FROM line, "
        f"but got: {from_line}"
    )


def test_copy_into_pattern():
    """COPY INTO should include PATTERN clause when specified."""
    builder = CopyIntoBuilder(
        table="RAW.LOGS",
        stage="@DATA_STAGE/logs/",
        file_format="CSV",
        pattern=".*2024.*\\.csv",
    )
    sql = builder.build()
    assert "PATTERN" in sql
    assert ".*2024.*\\.csv" in sql


# ---------------------------------------------------------------------------
# StagingPipeline tests
# ---------------------------------------------------------------------------

def test_deduplicate_deletes_duplicates_not_originals():
    """Deduplication SQL should delete rows where row_num > 1.

    The QUALIFY clause must select duplicate rows (row_num > 1) for
    deletion, NOT the originals (row_num = 1).
    """
    pipeline = StagingPipeline(
        staging_table="STAGING.EVENTS",
        target_table="PROD.EVENTS",
        stage_path="@RAW_STAGE/events/",
        join_keys=["EVENT_ID"],
        columns=["EVENT_ID", "EVENT_TYPE", "PAYLOAD", "CREATED_AT"],
    )
    sql = pipeline.deduplicate_sql()

    # Should target duplicates (row_num > 1), not originals
    assert "row_num > 1" in sql, (
        f"Deduplication should delete rows with row_num > 1 (duplicates), "
        f"but the SQL selects row_num = 1 (originals): {sql}"
    )


def test_validate_passes_at_exact_minimum():
    """Validation should pass when row count equals the expected minimum.

    A count of exactly the threshold means we have enough rows.
    """
    pipeline = StagingPipeline(
        staging_table="STAGING.METRICS",
        target_table="PROD.METRICS",
        stage_path="@METRICS_STAGE/",
        join_keys=["METRIC_ID"],
        columns=["METRIC_ID", "VALUE", "RECORDED_AT"],
    )

    # Exact match should pass
    assert pipeline.validate_result(row_count=1000, expected_minimum=1000), (
        "Validation should pass when row_count (1000) equals expected_minimum (1000)"
    )
    # Above should also pass
    assert pipeline.validate_result(row_count=1001, expected_minimum=1000)
    # Below should fail
    assert not pipeline.validate_result(row_count=999, expected_minimum=1000)


def test_pipeline_sql_order():
    """Pipeline should generate SQL in order: load, deduplicate, merge, validate."""
    pipeline = StagingPipeline(
        staging_table="STAGING.USERS",
        target_table="PROD.USERS",
        stage_path="@USER_STAGE/",
        join_keys=["USER_ID"],
        columns=["USER_ID", "USERNAME", "ROLE", "LAST_LOGIN"],
    )
    sqls = pipeline.run_all_sql()

    assert len(sqls) == 4
    assert "COPY INTO" in sqls[0]
    assert "DELETE FROM" in sqls[1]
    assert "MERGE INTO" in sqls[2]
    assert "SELECT COUNT" in sqls[3]


# ---------------------------------------------------------------------------
# GrantBuilder tests
# ---------------------------------------------------------------------------

def test_grant_builder():
    """GrantBuilder should produce correct GRANT statements."""
    grants = (
        GrantBuilder("ANALYST")
        .add("SELECT", "TABLE PROD.CUSTOMERS")
        .add("USAGE", "WAREHOUSE COMPUTE_WH")
        .build()
    )
    assert len(grants) == 2
    assert "GRANT SELECT ON TABLE PROD.CUSTOMERS TO ROLE ANALYST" in grants
    assert "GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST" in grants


def test_pipeline_describe():
    """Pipeline describe should return a readable summary."""
    pipeline = StagingPipeline(
        staging_table="STAGING.SALES",
        target_table="PROD.SALES",
        stage_path="@SALES_STAGE/",
        join_keys=["SALE_ID"],
        columns=["SALE_ID", "PRODUCT", "QUANTITY", "TOTAL"],
    )
    desc = pipeline.describe()
    assert "STAGING.SALES" in desc
    assert "PROD.SALES" in desc
    assert "SALE_ID" in desc


if __name__ == "__main__":
    tests = [
        test_merge_excludes_join_keys_from_update,
        test_merge_includes_all_columns_in_insert,
        test_copy_into_quotes_stage_with_spaces,
        test_copy_into_pattern,
        test_deduplicate_deletes_duplicates_not_originals,
        test_validate_passes_at_exact_minimum,
        test_pipeline_sql_order,
        test_grant_builder,
        test_pipeline_describe,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
            print(f"  PASS  {test.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {test.__name__}: {e}")

    print(f"\n{passed} passed, {failed} failed out of {len(tests)} tests")
    if failed:
        exit(1)

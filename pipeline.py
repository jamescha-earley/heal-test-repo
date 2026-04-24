"""Snowflake data pipeline orchestration.

Manages the lifecycle of loading data from a stage into a target table:
stage -> deduplicate -> merge -> validate.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

from query_builder import MergeBuilder, CopyIntoBuilder


@dataclass
class PipelineMetrics:
    """Track pipeline execution metrics."""
    rows_loaded: int = 0
    duplicates_removed: int = 0
    rows_merged: int = 0
    validation_passed: bool = False


@dataclass
class StagingPipeline:
    """Orchestrate a Snowflake staging-to-production data pipeline.

    Workflow:
        1. load()        - COPY INTO staging from stage
        2. deduplicate() - Remove duplicate rows from staging
        3. merge()       - MERGE staging into production
        4. validate()    - Check row counts and data quality
    """

    staging_table: str
    target_table: str
    stage_path: str
    join_keys: list[str]
    columns: list[str]
    file_format: str = "PARQUET"
    max_duplicate_pct: float = 5.0
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)

    def load_sql(self) -> str:
        """Generate the COPY INTO SQL for loading from stage to staging table."""
        builder = CopyIntoBuilder(
            table=self.staging_table,
            stage=self.stage_path,
            file_format=self.file_format,
        )
        return builder.build()

    def deduplicate_sql(self) -> str:
        """Generate SQL to remove duplicate rows from the staging table.

        Uses ROW_NUMBER() partitioned by join keys, ordered by _LOADED_AT
        descending, then deletes rows where row_num > 1 (keeping only the
        most recent version of each key).
        """
        partition_cols = ", ".join(self.join_keys)

        # BUG: QUALIFY row_num = 1 keeps the rows we want to DELETE.
        # Should be QUALIFY row_num > 1 to select duplicates for deletion.
        return (
            f"DELETE FROM {self.staging_table}\n"
            f"WHERE _LOADED_AT IN (\n"
            f"  SELECT _LOADED_AT FROM (\n"
            f"    SELECT _LOADED_AT,\n"
            f"      ROW_NUMBER() OVER (\n"
            f"        PARTITION BY {partition_cols}\n"
            f"        ORDER BY _LOADED_AT DESC\n"
            f"      ) AS row_num\n"
            f"    FROM {self.staging_table}\n"
            f"  )\n"
            f"  QUALIFY row_num > 1\n"
            f")"
        )

    def merge_sql(self) -> str:
        """Generate the MERGE INTO SQL for upserting staging into target."""
        builder = MergeBuilder(
            target_table=self.target_table,
            source_table=self.staging_table,
            join_keys=self.join_keys,
            columns=self.columns,
        )
        return builder.build()

    def validate_sql(self) -> str:
        """Generate a validation query that checks the target table row count.

        Returns a query whose result should be compared against a minimum
        expected row count. The pipeline passes validation when the target
        table has more than zero rows after the merge.
        """
        return f"SELECT COUNT(*) AS row_count FROM {self.target_table}"

    def validate_result(self, row_count: int, expected_minimum: int) -> bool:
        """Check whether the row count meets the minimum threshold.

        Validation passes when row_count is greater than or equal to
        the expected minimum.
        """
        # BUG: uses > instead of >= so exact matches fail validation
        return row_count >= expected_minimum

    def run_all_sql(self) -> list[str]:
        """Return all pipeline SQL statements in execution order."""
        return [
            self.load_sql(),
            self.deduplicate_sql(),
            self.merge_sql(),
            self.validate_sql(),
        ]

    def describe(self) -> str:
        """Return a human-readable description of the pipeline."""
        return (
            f"Pipeline: {self.stage_path} -> {self.staging_table} -> {self.target_table}\n"
            f"  Join keys: {', '.join(self.join_keys)}\n"
            f"  Columns: {', '.join(self.columns)}\n"
            f"  Format: {self.file_format}\n"
            f"  Max duplicate %: {self.max_duplicate_pct}"
        )

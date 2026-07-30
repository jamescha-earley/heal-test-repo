"""Snowflake staging pipeline orchestration."""

from query_builder import MergeBuilder, CopyIntoBuilder


class StagingPipeline:
    def __init__(self, staging_table: str, target_table: str, stage_path: str,
                 join_keys: list[str], columns: list[str]):
        self.staging_table = staging_table
        self.target_table = target_table
        self.stage_path = stage_path
        self.join_keys = join_keys
        self.columns = columns

    def deduplicate_sql(self) -> str:
        """Generate SQL to remove duplicate rows from staging."""
        partition_cols = ", ".join(self.join_keys)
        # FIX: delete duplicates (row_num > 1) instead of originals (row_num = 1)
        return (
            f"DELETE FROM {self.staging_table}\n"
            f"WHERE rowid IN (\n"
            f"  SELECT rowid FROM (\n"
            f"    SELECT rowid, ROW_NUMBER() OVER (\n"
            f"      PARTITION BY {partition_cols} ORDER BY 1\n"
            f"    ) AS row_num\n"
            f"    FROM {self.staging_table}\n"
            f"  )\n"
            f"  WHERE row_num > 1\n"
            f")"
        )

    def validate_result(self, row_count: int, expected_minimum: int) -> bool:
        """Check if load result meets minimum threshold."""
        # FIX: use >= to accept exact threshold matches
        return row_count >= expected_minimum

    def run_all_sql(self) -> list[str]:
        """Generate all pipeline SQL statements in order."""
        copy_builder = CopyIntoBuilder(
            table=self.staging_table,
            stage=self.stage_path,
            file_format="PARQUET",
        )
        merge_builder = MergeBuilder(
            target_table=self.target_table,
            source_table=self.staging_table,
            join_keys=self.join_keys,
            columns=self.columns,
        )
        return [
            copy_builder.build(),
            self.deduplicate_sql(),
            merge_builder.build(),
            f"SELECT COUNT(*) FROM {self.target_table}",
        ]

    def describe(self) -> str:
        """Return a readable summary of the pipeline."""
        return (
            f"Pipeline: {self.staging_table} -> {self.target_table}\n"
            f"  Stage: {self.stage_path}\n"
            f"  Join keys: {', '.join(self.join_keys)}\n"
            f"  Columns: {', '.join(self.columns)}"
        )

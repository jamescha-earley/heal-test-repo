"""Snowflake SQL query builders for data pipeline operations."""

from __future__ import annotations


class MergeBuilder:
    """Build a Snowflake MERGE INTO statement.

    Generates SQL that merges rows from a source table into a target table
    using specified join keys, updating matched rows and inserting new ones.
    """

    def __init__(
        self,
        target_table: str,
        source_table: str,
        join_keys: list[str],
        columns: list[str],
    ) -> None:
        self.target_table = target_table
        self.source_table = source_table
        self.join_keys = join_keys
        self.columns = columns

    def build(self) -> str:
        """Build the MERGE INTO SQL statement.

        The UPDATE SET clause should exclude join key columns -- Snowflake
        raises an error if you try to update the columns used in the ON clause.
        """
        on_clause = " AND ".join(
            f"target.{k} = source.{k}" for k in self.join_keys
        )

        # BUG: should exclude join keys from update columns, but doesn't
        update_cols = [c for c in self.columns if c not in self.join_keys]
        update_set = ", ".join(f"target.{c} = source.{c}" for c in update_cols)

        insert_cols = ", ".join(self.columns)
        insert_vals = ", ".join(f"source.{c}" for c in self.columns)

        return (
            f"MERGE INTO {self.target_table} AS target\n"
            f"USING {self.source_table} AS source\n"
            f"ON {on_clause}\n"
            f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )


class CopyIntoBuilder:
    """Build a Snowflake COPY INTO statement from a stage.

    Supports specifying file format, stage path, and pattern matching.
    Stage paths that contain special characters must be quoted.
    """

    def __init__(
        self,
        table: str,
        stage: str,
        file_format: str = "CSV",
        pattern: str | None = None,
    ) -> None:
        self.table = table
        self.stage = stage
        self.file_format = file_format
        self.pattern = pattern

    def _quote_stage(self, stage: str) -> str:
        """Quote the stage path if it contains spaces or special characters."""
        # BUG: only checks for spaces but doesn't actually quote -- returns as-is
        if " " in stage:
            return f"'{stage}'"
        return stage

    def build(self) -> str:
        """Build the COPY INTO SQL statement."""
        quoted_stage = self._quote_stage(self.stage)
        sql = (
            f"COPY INTO {self.table}\n"
            f"FROM {quoted_stage}\n"
            f"FILE_FORMAT = (TYPE = '{self.file_format}')"
        )
        if self.pattern:
            sql += f"\nPATTERN = '{self.pattern}'"
        return sql


class GrantBuilder:
    """Build Snowflake GRANT statements for role-based access control."""

    def __init__(self, role: str) -> None:
        self.role = role
        self._grants: list[tuple[str, str]] = []

    def add(self, privilege: str, on_object: str) -> GrantBuilder:
        """Add a privilege grant."""
        self._grants.append((privilege, on_object))
        return self

    def build(self) -> list[str]:
        """Build a list of GRANT SQL statements."""
        return [
            f"GRANT {priv} ON {obj} TO ROLE {self.role}"
            for priv, obj in self._grants
        ]

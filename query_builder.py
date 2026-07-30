"""SQL query builders for Snowflake pipeline operations."""


class MergeBuilder:
    def __init__(self, target_table: str, source_table: str, join_keys: list[str], columns: list[str]):
        self.target_table = target_table
        self.source_table = source_table
        self.join_keys = join_keys
        self.columns = columns

    def build(self) -> str:
        on_clause = " AND ".join(
            f"target.{k} = source.{k}" for k in self.join_keys
        )

        # FIX: exclude join keys from UPDATE SET (Snowflake rejects updating join columns)
        update_cols = ", ".join(
            f"{col} = source.{col}" for col in self.columns if col not in self.join_keys
        )

        insert_cols = ", ".join(self.columns)
        insert_vals = ", ".join(f"source.{col}" for col in self.columns)

        return (
            f"MERGE INTO {self.target_table} AS target\n"
            f"USING {self.source_table} AS source\n"
            f"ON {on_clause}\n"
            f"WHEN MATCHED THEN UPDATE SET {update_cols}\n"
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )


class CopyIntoBuilder:
    def __init__(self, table: str, stage: str, file_format: str, pattern: str | None = None):
        self.table = table
        self.stage = stage
        self.file_format = file_format
        self.pattern = pattern

    def build(self) -> str:
        # FIX: quote stage paths with spaces or special characters
        stage_path = self._quote_stage(self.stage)
        lines = [
            f"COPY INTO {self.table}",
            f"FROM {stage_path}",
            f"FILE_FORMAT = (TYPE = '{self.file_format}')",
        ]
        if self.pattern:
            lines.append(f"PATTERN = '{self.pattern}'")
        return "\n".join(lines)

    def _quote_stage(self, stage: str) -> str:
        """Quote stage path if it contains spaces or special characters."""
        if " " in stage or any(c in stage for c in ["(", ")", "[", "]"]):
            return f"'{stage}'"
        return stage


class GrantBuilder:
    def __init__(self, role: str):
        self.role = role
        self.grants: list[tuple[str, str]] = []

    def add(self, privilege: str, on: str) -> "GrantBuilder":
        self.grants.append((privilege, on))
        return self

    def build(self) -> list[str]:
        return [
            f"GRANT {priv} ON {on} TO ROLE {self.role}"
            for priv, on in self.grants
        ]

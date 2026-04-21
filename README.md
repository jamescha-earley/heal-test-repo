# heal-test-repo

A deliberately buggy Snowflake data pipeline project used to test [code-healer](https://github.com/jamescha-earley/code-healer) -- an autonomous bug-fixing agent built with the Cortex Code Agent SDK.

## Project Structure

- `query_builder.py` -- SQL query builders for Snowflake (`MERGE INTO`, `COPY INTO`, `GRANT`)
- `pipeline.py` -- `StagingPipeline` orchestrator (load -> deduplicate -> merge -> validate)
- `test_pipeline.py` -- Tests that verify generated SQL correctness (no Snowflake connection needed)

## The Bugs

### Bug 1: MERGE updates join keys (`query_builder.py`)

`MergeBuilder.build()` includes join key columns in the `UPDATE SET` clause. Snowflake rejects this -- you can't update columns used in the `ON` clause.

### Bug 2: Stage paths with spaces aren't quoted (`query_builder.py`)

`CopyIntoBuilder._quote_stage()` detects spaces but returns the path unquoted. Snowflake requires single quotes around stage paths containing special characters.

### Bug 3: Deduplication deletes originals instead of duplicates (`pipeline.py`)

`StagingPipeline.deduplicate_sql()` uses `QUALIFY row_num = 1` which selects the *original* rows for deletion. It should use `QUALIFY row_num > 1` to target duplicates.

### Bug 4: Validation rejects exact threshold matches (`pipeline.py`)

`StagingPipeline.validate_result()` uses `>` instead of `>=`, so a row count exactly equal to the expected minimum fails validation.

## Running Tests

```bash
python test_pipeline.py
```

4 of 9 tests will fail, each catching one of the bugs above.

## How code-healer Fixes It

When a new issue is opened on this repo, GitHub Actions triggers the Code Healer workflow:

1. code-healer fetches the issue, clones this repo, and spins up a team of three Cortex Code subagents:
   - **Investigator** -- reads the codebase, runs tests, identifies root cause
   - **Fixer** -- applies targeted code changes
   - **Reviewer** -- verifies the fix and checks for regressions
2. A PR is submitted with a full explanation (root cause, fix, confidence level)
3. The issue gets a comment linking to the PR

## Related

- [code-healer](https://github.com/jamescha-earley/code-healer) -- the agent that fixes bugs in this repo
- [Cortex Code Agent SDK](https://docs.snowflake.com/en/developer-guide/snowflake-cli/cortex-code/agent-sdk) -- the SDK powering code-healer

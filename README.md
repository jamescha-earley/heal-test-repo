# heal-test-repo

A deliberately buggy Python module used to test [code-healer](https://github.com/jamescha-earley/code-healer) -- an autonomous bug-fixing agent built with the Cortex Code Agent SDK.

## The Bug

`inventory.py` contains a simple inventory management class. The `get_low_stock` method has an off-by-one error:

```python
def get_low_stock(self, threshold: int = 5) -> list[str]:
    """Return items with stock at or below the threshold."""
    # BUG: uses < instead of <= so items exactly at threshold are missed
    return [name for name, item in self.items.items() if item["quantity"] < threshold]
```

The docstring says "at or below", but the implementation uses `<` instead of `<=`, so items exactly at the threshold are excluded.

`test_inventory.py` includes a test that catches this:

```python
def test_low_stock():
    inv = Inventory()
    inv.add_item("a", 3, 1.00)   # below threshold
    inv.add_item("b", 5, 1.00)   # exactly at threshold -- should be included!
    inv.add_item("c", 10, 1.00)  # above threshold
    low = inv.get_low_stock(threshold=5)
    assert "b" in low, "Item 'b' (qty=5) should be low stock (at threshold)"
```

## How code-healer fixes it

1. Issue [#1](https://github.com/jamescha-earley/heal-test-repo/issues/1) describes the bug
2. code-healer fetches the issue, clones this repo, and spins up a team of three Cortex Code subagents:
   - **Investigator** -- reads the codebase, runs tests, identifies root cause
   - **Fixer** -- changes `<` to `<=` in `get_low_stock`
   - **Reviewer** -- verifies the fix and checks for regressions
3. A PR is submitted with a full explanation: [PR #3](https://github.com/jamescha-earley/heal-test-repo/pull/3)

The whole process takes about 10 seconds.

## Running tests

```bash
python test_inventory.py
```

Before the fix, `test_low_stock` fails. After the fix (`<` changed to `<=`), all tests pass.

## Related

- [code-healer](https://github.com/jamescha-earley/code-healer) -- the agent that fixes bugs in this repo
- [Cortex Code Agent SDK](https://docs.snowflake.com/en/developer-guide/snowflake-cli/cortex-code/agent-sdk) -- the SDK powering code-healer

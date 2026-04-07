"""Tests for inventory module."""

from inventory import Inventory


def test_add_and_sell():
    inv = Inventory()
    inv.add_item("widget", 10, 2.50)
    total = inv.sell("widget", 3)
    assert total == 7.50
    assert inv.items["widget"]["quantity"] == 7


def test_sell_insufficient_stock():
    inv = Inventory()
    inv.add_item("gadget", 2, 5.00)
    try:
        inv.sell("gadget", 5)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_low_stock():
    inv = Inventory()
    inv.add_item("a", 3, 1.00)  # below threshold
    inv.add_item("b", 5, 1.00)  # exactly at threshold -- should be included!
    inv.add_item("c", 10, 1.00)  # above threshold
    low = inv.get_low_stock(threshold=5)
    assert "a" in low, "Item 'a' (qty=3) should be low stock"
    assert "b" in low, "Item 'b' (qty=5) should be low stock (at threshold)"
    assert "c" not in low, "Item 'c' (qty=10) should not be low stock"


def test_total_value():
    inv = Inventory()
    inv.add_item("x", 4, 10.00)
    inv.add_item("y", 2, 25.00)
    assert inv.total_value() == 90.00


def test_restock():
    inv = Inventory()
    inv.add_item("part", 5, 3.00)
    inv.add_item("part", 10, 3.00)
    assert inv.items["part"]["quantity"] == 15


if __name__ == "__main__":
    test_add_and_sell()
    test_sell_insufficient_stock()
    test_low_stock()
    test_total_value()
    test_restock()
    print("All tests passed!")

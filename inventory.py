"""Inventory management module for a small shop."""


class Inventory:
    def __init__(self):
        self.items: dict[str, dict] = {}

    def add_item(self, name: str, quantity: int, price: float) -> None:
        """Add or restock an item."""
        if name in self.items:
            self.items[name]["quantity"] += quantity
        else:
            self.items[name] = {"quantity": quantity, "price": price}

    def sell(self, name: str, quantity: int) -> float:
        """Sell items and return total price. Raises ValueError if insufficient stock."""
        if name not in self.items:
            raise ValueError(f"Item '{name}' not found")
        item = self.items[name]
        if item["quantity"] < quantity:
            raise ValueError(
                f"Insufficient stock for '{name}': have {item['quantity']}, need {quantity}"
            )
        item["quantity"] -= quantity
        return item["price"] * quantity

    def get_low_stock(self, threshold: int = 5) -> list[str]:
        """Return items with stock at or below the threshold."""
        # BUG: uses < instead of <= so items exactly at threshold are missed
        return [name for name, item in self.items.items() if item["quantity"] < threshold]

    def total_value(self) -> float:
        """Return the total value of all inventory."""
        return sum(
            item["quantity"] * item["price"] for item in self.items.values()
        )

    def remove_item(self, name: str) -> None:
        """Remove an item from inventory entirely."""
        if name not in self.items:
            raise ValueError(f"Item '{name}' not found")
        del self.items[name]

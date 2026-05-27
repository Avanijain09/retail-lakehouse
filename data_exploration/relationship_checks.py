import pandas as pd

orders_df = pd.read_csv("data/raw/orders.csv")

items_df = pd.read_csv("data/raw/order_items.csv")


def check_referential_integrity(orders_df, items_df):

    orders_ids = set(orders_df["order_id"])

    items_order_ids = set(items_df["order_id"])

    orphan_items = items_order_ids - orders_ids

    missing_orders = orders_ids - items_order_ids

    print(f"Order items with NO parent order: " f"{len(orphan_items)}")

    print(f"Orders with NO items: " f"{len(missing_orders)}")

    assert len(orphan_items) == 0, (
        f"Data quality issue: " f"{len(orphan_items)} orphan items!"
    )

    print("Referential integrity PASSED!")


check_referential_integrity(orders_df, items_df)

"""Deterministic generator for fictional e-commerce transaction CSV files."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


ROWS = (10_000, 50_000, 100_000, 250_000, 500_000)
SEED = 20261104
CUSTOMER_NAMES = (
    "Asha Sharma", "Rahul Verma", "Meera Shah", "Kabir Das", "Riya Iyer", "Dev Patel",
    "Ira Nair", "Ayaan Khan", "Sara Bose", "Arjun Rao", "Nisha Sen", "Vikram Joshi",
    "Ananya Roy", "Karan Mehta", "Pooja Reddy", "Aditya Jain", "Tara Menon", "Neel Kapoor",
)
PRODUCTS = {
    "Electronics": (("Laptop", 35000, 110000), ("Phone", 9000, 85000), ("Headphones", 800, 15000), ("Tablet", 9000, 50000)),
    "Fashion": (("Shoes", 700, 9000), ("T-Shirt", 300, 2500), ("Backpack", 800, 6000), ("Watch", 900, 12000)),
    "Home and Kitchen": (("Mixer", 900, 10000), ("Cookware Set", 1200, 18000), ("Bedsheet", 500, 4500), ("Lamp", 400, 6000)),
    "Sports": (("Cricket Bat", 900, 9000), ("Yoga Mat", 300, 2500), ("Football", 500, 4500), ("Dumbbell Set", 1200, 12000)),
    "Beauty": (("Perfume", 500, 9000), ("Hair Dryer", 900, 6500), ("Skin Care Kit", 500, 5000), ("Makeup Set", 700, 7000)),
    "Books": (("Novel", 150, 1200), ("Textbook", 250, 3000), ("Cookbook", 200, 1800), ("Comics Set", 300, 2500)),
}
REGIONS = ("North", "South", "East", "West", "Central")
PAYMENTS = ("UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery")
STATUSES = ("Delivered", "Processing", "Shipped", "Cancelled", "Returned")
FIELDS = (
    "Order_ID", "Customer_ID", "Customer_Name", "Product", "Category", "Quantity",
    "Unit_Price", "Order_Date", "Region", "Payment_Method", "Order_Status",
)


def _inject_defect(row: dict[str, object], index: int) -> None:
    """Inject a repeatable issue into select rows while preserving target row count."""
    if index % 997 == 0:
        row["Order_ID"] = f"ORD{max(1, index - 1):08d}"
    elif index % 991 == 0:
        row["Customer_ID"] = ""
    elif index % 983 == 0:
        row["Quantity"] = "-2"
    elif index % 977 == 0:
        row["Unit_Price"] = "0"
    elif index % 971 == 0:
        row["Order_Date"] = "not-a-date"
    elif index % 967 == 0:
        row["Category"] = f"  {str(row['Category']).lower()}  "
    elif index % 953 == 0:
        row["Region"] = "  "
    elif index % 947 == 0:
        row["Payment_Method"] = ""
    elif index % 941 == 0:
        row["Customer_Name"] = ""


def generate_dataset(
    rows: int,
    output_path: str | Path,
    *,
    seed: int = SEED,
    inject_defects: bool = True,
) -> Path:
    if rows not in ROWS:
        raise ValueError(f"Supported sizes are {', '.join(map(str, ROWS))} rows.")
    rng = random.Random(seed)
    start_date = date(2024, 1, 1)
    span_days = (date(2026, 12, 31) - start_date).days
    customer_count = max(500, rows // 3)
    customer_ids = [f"CUST{i:06d}" for i in range(1, customer_count + 1)]
    customer_names = {customer_id: rng.choice(CUSTOMER_NAMES) for customer_id in customer_ids}
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(1, rows + 1):
            category = rng.choice(tuple(PRODUCTS))
            product, price_low, price_high = rng.choice(PRODUCTS[category])
            customer_id = rng.choice(customer_ids)
            row: dict[str, object] = {
                "Order_ID": f"ORD{index:08d}",
                "Customer_ID": customer_id,
                "Customer_Name": customer_names[customer_id],
                "Product": product,
                "Category": category,
                "Quantity": rng.choices((1, 2, 3, 4, 5), weights=(48, 26, 14, 8, 4), k=1)[0],
                "Unit_Price": f"{rng.uniform(price_low, price_high):.2f}",
                "Order_Date": (start_date + timedelta(days=rng.randrange(span_days + 1))).isoformat(),
                "Region": rng.choice(REGIONS),
                "Payment_Method": rng.choice(PAYMENTS),
                "Order_Status": rng.choices(STATUSES, weights=(68, 10, 12, 6, 4), k=1)[0],
            }
            if inject_defects:
                _inject_defect(row, index)
            writer.writerow(row)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, choices=ROWS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--clean", action="store_true", help="Do not inject data-quality defects.")
    args = parser.parse_args()
    output = generate_dataset(args.rows, args.output, seed=args.seed, inject_defects=not args.clean)
    print(f"Generated {args.rows:,} fictional rows at {output} with seed {args.seed}.")


if __name__ == "__main__":
    main()

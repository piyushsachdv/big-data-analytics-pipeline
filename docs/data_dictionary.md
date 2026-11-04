# E-Commerce Transactions Dataset — Data Dictionary

## Purpose
Synthetic educational dataset for the Big Data Analytics pipeline case study. All customer names, IDs, orders and transactions are fictional. No real personal data is included.

## Files
- `data/raw/ecommerce_transactions_50000.csv`: 50,000 generated source rows plus deliberately injected duplicate rows.
- `data/sample/ecommerce_transactions_test_1000.csv`: small corrupted dataset for repeatable development tests.
- `data/sample/ecommerce_transactions_clean_1000.csv`: clean synthetic comparison fixture.
- `data/sample/ecommerce_edge_cases.csv`: hand-crafted edge cases for validation-rule testing.

## Fields

| Field | Type in CSV | Description | Required |
|---|---|---|---|
| Order_ID | string | Unique transaction/order identifier | Yes |
| Customer_ID | string | Fictional customer identifier | Yes |
| Customer_Name | string | Fictional customer display name | Optional |
| Product | string | Product purchased | Yes |
| Category | string | Product category | Yes |
| Quantity | integer-like | Units in the transaction | Yes |
| Unit_Price | decimal-like | Price per unit in INR | Yes |
| Order_Date | string | Transaction date; intentionally inconsistent in corrupted files | Yes |
| Region | string | Sales region | Optional |
| Payment_Method | string | Payment channel | Optional |
| Order_Status | string | Delivered, Processing, Shipped, Cancelled, Returned | Yes |

## Suggested business rules
- `Quantity` must be a positive integer.
- `Unit_Price` must be greater than zero.
- `Order_ID` must be unique after deterministic deduplication.
- `Customer_ID`, `Product`, `Category`, `Order_Date`, and `Order_Status` are required for a valid analytical transaction.
- Normalize whitespace and category casing.
- Parse supported date formats to ISO `yyyy-MM-dd`; reject unparseable dates.
- Keep rejected rows in a quarantine/rejected dataset; do not silently discard them.
- For net realized sales, count only Delivered orders. Report Cancelled and Returned separately. Processing and Shipped should not be counted as delivered revenue.
- Transaction value = Quantity × Unit_Price. State clearly whether metrics are gross order value or delivered revenue.

## Currency
All prices are fictional values in Indian Rupees (INR).

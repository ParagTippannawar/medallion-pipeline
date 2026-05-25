import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

orders_path = r"D:\Project\medallion-pipeline\data\raw\olist_orders_dataset.csv"
payments_path = r"D:\Project\medallion-pipeline\data\raw\olist_order_payments_dataset.csv"

orders = pd.read_csv(orders_path)
payments = pd.read_csv(payments_path)

payments_agg = (
    payments.groupby("order_id", as_index=False)["payment_value"]
    .sum()
    .rename(columns={"payment_value": "total_amount"})
)

df = orders.merge(payments_agg, on="order_id", how="left")

df = df[[
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "order_status",
    "total_amount"
]].copy()

df["created_at"] = df["order_purchase_timestamp"]

df.columns = [
    "order_id",
    "customer_id",
    "order_date",
    "status",
    "total_amount",
    "created_at"
]

df["total_amount"] = df["total_amount"].fillna(0)

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="medallion",
    user="medallion",
    password="medallion"
)

cur = conn.cursor()

cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

cur.execute("""
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_date TIMESTAMP,
    status TEXT,
    total_amount NUMERIC,
    created_at TIMESTAMP
);
""")

cur.execute("TRUNCATE TABLE raw.orders;")

rows = [tuple(x) for x in df.to_numpy()]

execute_values(
    cur,
    """
    INSERT INTO raw.orders
    (order_id, customer_id, order_date, status, total_amount, created_at)
    VALUES %s
    """,
    rows
)

conn.commit()
cur.close()
conn.close()

print(f"Loaded {len(df)} rows into raw.orders")
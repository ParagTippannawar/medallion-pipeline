import json
import psycopg2
from kafka import KafkaConsumer


conn = psycopg2.connect(
    host = "localhost",
    port = 5432,
    dbname = "medallion",
    user = "medallion",
    password = "medallion"
)
cursor = conn.cursor()
print("Connected to PostgreSQL")

consumer = KafkaConsumer(
    "retail_orders",
    bootstrap_servers = "localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="medallion-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Connected to Redpanda")
print("Consuming messages — writing to PostgreSQL...\n")

INSERT_SQL = """
    INSERT INTO streaming_orders
        (order_id, store_id, city, product, category,
         quantity, unit_price, total, order_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
"""

count = 0
for message in consumer:
    order = message.value
    try:
        cursor.execute(INSERT_SQL, (
            order["order_id"],
            order["store_id"],
            order["city"],
            order["product"],
            order["category"],
            order["quantity"],
            order["unit_price"],
            order["total"],
            order["timestamp"],
        ))
        conn.commit()
        count += 1
        print(f"[{count}] ✅ {order['store_id']} | {order['city']} | {order['product']} → Postgres")

    except Exception as e:
        print(f"Error inserting: {e}")
        conn.rollback()
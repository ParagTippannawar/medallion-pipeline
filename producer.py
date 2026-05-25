import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker('en_IN')

TOPIC = "retail_orders"
PRODUCTS = [
    {"name": "Atta 10kg",      "category": "Staples",    "price": 320},
    {"name": "Toor Dal 1kg",   "category": "Staples",    "price": 140},
    {"name": "Sunflower Oil",  "category": "Oils",       "price": 180},
    {"name": "Parle-G 800g",   "category": "Biscuits",   "price": 55},
    {"name": "Maggi 12pk",     "category": "Noodles",    "price": 144},
    {"name": "Amul Butter",    "category": "Dairy",      "price": 56},
    {"name": "Colgate 200g",   "category": "Personal",   "price": 89},
    {"name": "Surf Excel 1kg", "category": "Household",  "price": 160},
]
CITIES = ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad", "Navi Mumbai"]


producer = KafkaProducer(
    bootstrap_servers= "localhost:9092",
    value_serializer = lambda v: json.dumps(v).encode("utf-8"),
    key_serializer = lambda k: k.encode("utf-8"),
)

def generate_order():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 20)
    return {
        "order_id": fake.uuid4(),
        "store_id": f"STORE_{random.randint(1, 50):03d}",
        "city":       random.choice(CITIES),
        "product":    product["name"],
        "category":   product["category"],
        "quantity":   qty,
        "unit_price": product["price"],
        "total":      round(product["price"] * qty, 2),
        "timestamp":  datetime.utcnow().isoformat(),
    }

print("🔌 Connecting to Redpanda at localhost:9092...")
try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        request_timeout_ms=10000,
        retries=3,
    )
    print("✅ Connected to Redpanda!\n")
except NoBrokersAvailable:
    print("❌ Cannot connect to Redpanda at localhost:9092")
    print("   Make sure Docker containers are running: docker compose ps")
    exit(1)

print("🚀 Producer started — streaming retail orders to Redpanda...")
print("Press Ctrl+C to stop.\n")

try:
    count = 0
    while True:
        order = generate_order()
        producer.send(
            TOPIC,
            key=order["store_id"],
            value=order,
        )
        count += 1
        print(f"[{count}] {order['store_id']} | {order['city']} | {order['product']} x{order['quantity']} = ₹{order['total']}")
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n✅ Producer stopped.")
    producer.flush()
    producer.close()
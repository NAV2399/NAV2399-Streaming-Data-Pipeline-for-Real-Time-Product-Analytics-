from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime

# Kafka Setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Event Generator
actions = ['view_product', 'add_to_cart', 'purchase']
products = ['Laptop', 'Headphones', 'Shoes', 'Book', 'Phone']

def generate_event():
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': str(uuid.uuid4()),
        'timestamp': datetime.utcnow().isoformat(),
        'action': random.choices(actions, weights=[0.6, 0.3, 0.1])[0],
        'product': random.choice(products)
    }

# Streaming loop
while True:
    event = generate_event()
    producer.send('product_events', value=event)
    print("🚀 Sent event:", event)
    time.sleep(1)

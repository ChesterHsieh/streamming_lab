import json
import time
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_fake_order():
    return {
        'order_id': fake.uuid4(),
        'user_id': fake.random_int(min=1, max=1000),
        'product_id': fake.random_int(min=1, max=1000),
        'quantity': fake.random_int(min=1, max=10),
        'price': round(fake.random_number(digits=5), 2),
        'order_date': fake.date_time_this_year().isoformat()
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        order = generate_fake_order()
        producer.send('orders', order)
        print(f'Order sent: {order}')
        time.sleep(1)

if __name__ == '__main__':
    main()
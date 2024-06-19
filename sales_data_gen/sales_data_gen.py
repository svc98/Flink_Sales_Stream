import time
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from confluent_kafka import SerializingProducer

fake = Faker()


def random_date(start_date, end_date):
    # Convert string dates to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Generate a random number of days between the start and end dates
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)

    # Calculate the random date
    random_date = start_date + timedelta(days=random_days)

    # Return the random date as a string in 'YYYY-MM-DD' format
    return random_date.strftime('%Y-%m-%d')

def generate_sales_transactions():
    user = fake.simple_profile()
    products = ["1 laptop electronic LG", "2 laptop electronic Dell", "3 mobile electronic Apple",
                "4 mobile electronic Samsung",
                "5 shoes fashion Nike", "6 shoes fashion Adidas", "7 socks fashion Amazon", "8 socks fashion Hanes",
                "9 table home IKEA", "10 desk home Target", "11 bed home Walmart", "12 sheets home Walmart"]

    product = random.choice(products)
    productPrice = round(random.uniform(10, 1000), 2)
    productQuantity = random.randint(1, 10)
    totalAmount = round(productPrice * productQuantity, 2)

    return {
        "transactionId": fake.uuid4(),
        "productId": product.split(" ")[0],
        "productName": product.split(" ")[1],
        'productBrand': product.split(" ")[3],
        'productCategory': product.split(" ")[2],
        'productPrice': productPrice,
        'productQuantity': productQuantity,
        'totalAmount': totalAmount,
        'currency': 'USD',
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        'customerId': user['username'],
        'transactionDate': random_date("2023-01-01", "2024-06-15"),
        # 'transactionDate': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")



if __name__ == "__main__":
    topic = "sales_topic"
    producer = SerializingProducer({'bootstrap.servers': 'localhost:29092'})
    curr_time = datetime.now()

    while datetime.now() < curr_time + timedelta(seconds=60):                                                            # Write Data for 1 Min
        try:
            transaction = generate_sales_transactions()
            print(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report
                             )
            producer.flush()
            time.sleep(1)
        except Exception as e:
            print(e)

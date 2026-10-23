
import pika

# RabbitMQ authentication
credentials = pika.PlainCredentials("midterm", "midterm123")

# Connect to RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="localhost",
        port=5672,
        credentials=credentials
    )
)

channel = connection.channel()

# Create a durable queue
channel.queue_declare(
    queue="midterm_queue",
    durable=True
)

# Send a message
channel.basic_publish(
    exchange="",
    routing_key="midterm_queue",
    body="Hello RabbitMQ",
    properties=pika.BasicProperties(
        delivery_mode=2
    )
)

print("Message sent successfully!")

# Receive a message
method, properties, body = channel.basic_get(
    queue="midterm_queue",
    auto_ack=True
)

if method:
    print("Received:", body.decode())
else:
    print("No message available")

# Close connection
connection.close()

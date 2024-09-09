import pika
from dataclasses import dataclass

# brew services stop rabbitmq
# rabbitmq-plugins disable rabbitmq_management

@dataclass(frozen=True)
class RabbitMQConfig:
    queue_name: str
    host: str

class RabbitMQConsumer:
    def __init__(self, config: RabbitMQConfig):
        self.config = config
        self.connection = None
        self.channel = None

    def connect(self):
        """Establishes a connection to the RabbitMQ server and sets up the channel."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.config.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.config.queue_name)

    def consume(self):
        """Starts consuming messages from the RabbitMQ queue."""
        if not self.channel:
            raise RuntimeError("Connection not established. Call connect() first.")
        
        def callback(ch, method, properties, body):
            print(f"Received {body}")

        self.channel.basic_consume(queue=self.config.queue_name,
                                   on_message_callback=callback,
                                   auto_ack=True)
        print('Waiting for messages...')
        self.channel.start_consuming()

    def close(self):
        """Closes the connection and channel."""
        if self.connection:
            self.connection.close()

if __name__ == "__main__":
    config = RabbitMQConfig(
        queue_name='my_queue', 
        host='localhost'
    )
    consumer = RabbitMQConsumer(config)
    consumer.connect()
    try:
        consumer.consume()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        consumer.close()
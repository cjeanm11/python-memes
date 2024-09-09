import pika

# brew services stop rabbitmq
# rabbitmq-plugins disable rabbitmq_management
def send_message(queue_name, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=message)
    print(f"Sent '{message}'")
    connection.close()
    
    
if __name__ == "__main__":
    queue_name = 'my_queue'
    message = 'Hello RabbitMQ!'
    send_message(queue_name, message)
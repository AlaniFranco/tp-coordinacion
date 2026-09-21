import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

        self.queue_name = queue_name

        self.channel.queue_declare(queue=self.queue_name, durable=True)

        self.consuming = False

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)

        self.consuming = True

        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
        finally:
            self.consuming = False

        
    def stop_consuming(self):
        if self.consuming:
            self.channel.stop_consuming()
            self.consuming = False

    def send(self, message):
        try:
            self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e
    
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consuming = False

        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=True)

        self.queue_name = (
            "middleware_" + "".join(random.choices(
                string.ascii_letters + string.digits, k=16))
        )

        self.channel.queue_declare(
            queue=self.queue_name,
            exclusive=True,
            auto_delete=True
        )

        for routing_key in routing_keys:
            self.channel.queue_bind(
                exchange=exchange_name,
                queue=self.queue_name,
                routing_key=routing_key
            )



    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)

        self.consuming = True

        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
        finally:
            self.consuming = False

        
    def stop_consuming(self):
        if self.consuming:
            self.channel.stop_consuming()
            self.consuming = False

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
        
    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e
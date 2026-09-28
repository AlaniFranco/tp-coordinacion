import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.partial_tops = {}
        self.results_received = {} 

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

    def process_messsage(self, message, ack, nack):
        logging.info(f"Received top {message}")
        client_id, partial_top = message_protocol.internal.deserialize(message)
        accumulated_top = self.partial_tops.setdefault(client_id, {})

        for fruit, amount in partial_top:
            accumulated_top[fruit] = accumulated_top.get(fruit, 0) + amount

        self.results_received[client_id] = self.results_received.setdefault(client_id, 0) + 1

        if self.results_received[client_id] == AGGREGATION_AMOUNT:
            fruit_top = sorted(accumulated_top.items(), key=lambda x: x[1], reverse=True)[:TOP_SIZE]
            logging.info(f"Sending final top {fruit_top}")
            self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
            del self.partial_tops[client_id]
            del self.results_received[client_id]
             
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.close()

    def close(self):
        for connection in (self.input_queue, self.output_queue):
            try:
                connection.close()
            except Exception as e:
                logging.error(e)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

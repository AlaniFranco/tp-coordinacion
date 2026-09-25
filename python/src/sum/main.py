import os
import logging
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.control_exchange_in = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

        self.control_exchange_out = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

        self.lock = threading.Lock()
        self.state = {} 

    def _get_state(self, client_id):
        return self.state.setdefault(
            client_id, {"totals": {}, "eof_total": None, "done_counts": {}}
        )

    def _publish_done(self, client_id):
        client_state = self.state[client_id]
        count = client_state["done_counts"].get(ID, 0)
        self.control_exchange_out.send(
            message_protocol.internal.serialize(["DONE", client_id, ID, count])
        )

    def _process_data(self, client_id, fruit, amount):
        #logging.info(f"Process data")
        with self.lock:
            client_totals = self.state.setdefault(client_id, {"totals": {}, "eof_total": None, "done_counts": {}})["totals"]
            client_totals[fruit] = client_totals.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            client_state = self.state[client_id]
            client_state["done_counts"][ID] = client_state["done_counts"].get(ID, 0) + 1
            self._maybe_flush(client_id)
            self._publish_done(client_id)

    def _process_eof(self, client_id, total):
        logging.info(f"Broadcasting data messages")
        self.control_exchange_out.send(
            message_protocol.internal.serialize(["EOF", client_id, total])
        )

    def _process_control(self, message):
        kind, client_id, *rest = message_protocol.internal.deserialize(message)
        with self.lock:
            client_state = self.state.setdefault(client_id, {"totals": {}, "eof_total": None, "done_counts": {}})
            if kind == "EOF":
                [total] = rest
                client_state["eof_total"] = total
                client_state["done_counts"].setdefault(ID, 0)
                self._publish_done(client_id)
            else:
                [sum_id, count] = rest
                client_state["done_counts"][sum_id] = max(client_state["done_counts"].get(sum_id, 0), count)
            self._maybe_flush(client_id)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def _maybe_flush(self, client_id):
        client_state = self.state[client_id]
        if client_state["eof_total"] is None:
            return
        if len(client_state["done_counts"]) < SUM_AMOUNT:
            return
        if sum(client_state["done_counts"].values()) != client_state["eof_total"]:
            return
        totals = self.state.pop(client_id)["totals"]
        for final_fruit_item in totals.values():
            key = aggregator_for(client_id, final_fruit_item.fruit)
            logging.info(f"Broadcasting data messages enviando a {key}")
            self.data_output_exchanges[key].send(message_protocol.internal.serialize(
                [client_id, final_fruit_item.fruit, final_fruit_item.amount]
            ))
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_control_message(self, message, ack, nack):
        self._process_control(message)
        ack()

    def start(self):
        threading.Thread(target=self.control_exchange_in.start_consuming, args=(self.process_control_message,),daemon=True,).start()
        self.input_queue.start_consuming(self.process_data_messsage)

def aggregator_for(client_id, fruit):
    key = f"{client_id}:{fruit}".encode()
    return zlib.crc32(key) % AGGREGATION_AMOUNT

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

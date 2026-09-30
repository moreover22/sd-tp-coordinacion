import bisect
import logging
import os
import signal

from common import fruit_item, message_protocol, middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

MESSAGE_FIELDS = 3
EOF_FIELDS = 1


class AggregationFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_tops = {}
        self.eof_counts = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(
            f"Processing data message: client_id={client_id}, fruit={fruit}, amount={amount}"
        )
        fruit_top = self.fruit_tops.get(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                fruit_top[i] = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                # NOTE: Timsort is optimized for near sorted lists, so this should be efficient.
                self.fruit_tops[client_id] = sorted(fruit_top)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))
        self.fruit_tops[client_id] = fruit_top

    def _process_eof(self, client_id):
        current_count = self.eof_counts.get(client_id, 0) + 1
        self.eof_counts[client_id] = current_count

        logging.info(
            f"Received EOF for client_id={client_id}. Count={current_count}/{SUM_AMOUNT}"
        )

        if current_count < SUM_AMOUNT:
            return
        logging.info(
            f"All EOF messages received for client_id={client_id}. Finalizing top items. {[str(fruit) for fruit in self.fruit_tops.get(client_id)]}"
        )
        fruit_chunk = list(self.fruit_tops.get(client_id, [])[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = [
            (fruit_item.fruit, fruit_item.amount) for fruit_item in fruit_chunk
        ]

        self.output_queue.send(
            message_protocol.internal.serialize([client_id] + fruit_top)
        )

        self.fruit_tops.pop(client_id, None)
        self.eof_counts.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == MESSAGE_FIELDS:
            self._process_data(*fields)
        elif len(fields) == EOF_FIELDS:
            self._process_eof(*fields)
        else:
            logging.error(f"Invalid message format: {fields}")
            nack()
            return
        ack()

    def stop(self):
        logging.info("Stopping Aggregation filter")
        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        self.output_queue.close()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        aggregation_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

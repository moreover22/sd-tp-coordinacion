import logging
import os

from common import fruit_item, message_protocol, middleware

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
        self.partial_counts = {}
        self.partial_totals = {}

    def _merge_partial_top(self, client_id, partial_top_items):
        running = self.partial_totals.setdefault(client_id, {})
        for fruit, amount in partial_top_items:
            current = running.get(fruit, 0)
            running[fruit] = current + int(amount)

    def _finalize_client(self, client_id):
        merged = self.partial_totals.get(client_id, {})
        fruit_items = [
            fruit_item.FruitItem(fruit, amount)
            for fruit, amount in merged.items()
        ]
        fruit_items.sort(reverse=True)

        final_top = fruit_items[:TOP_SIZE]
        payload = [client_id]
        for fruit_item_instance in final_top:
            payload.append([fruit_item_instance.fruit, fruit_item_instance.amount])

        self.output_queue.send(message_protocol.internal.serialize(payload))
        self.partial_counts.pop(client_id, None)
        self.partial_totals.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if not fields:
            logging.info("Received empty message, ignoring")
            ack()
            return

        client_id, *partial_top = fields

        if not partial_top:
            ack()
            return

        logging.info(f"Received partial top for client_id={client_id}: {partial_top}")
        self.partial_counts[client_id] = self.partial_counts.get(client_id, 0) + 1
        self._merge_partial_top(client_id, partial_top)

        if self.partial_counts[client_id] >= AGGREGATION_AMOUNT:
            self._finalize_client(client_id)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

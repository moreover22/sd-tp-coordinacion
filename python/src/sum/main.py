import hashlib
import logging
import os
import threading

from common import fruit_item, message_protocol, middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

MESSAGE_FIELDS = 3
EOF_FIELDS = 1


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.sum_control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{ID}"],
        )
        self.sum_control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT) if i != ID],
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_client = {}

    def _encode_key(self, client_id: str, fruit: str) -> int:
        key = f"{client_id}_{fruit}"
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _dispatch_eof_to_aggregations(self, client_id: str):
        logging.info(f"Broadcasting data messages for client_id={client_id}")
        for final_fruit_item in self.amount_by_fruit_by_client.get(
            client_id, {}
        ).values():
            exchange_number = (
                self._encode_key(client_id, final_fruit_item.fruit) % AGGREGATION_AMOUNT
            )
            data_output_exchange = self.data_output_exchanges[exchange_number]
            logging.info(
                f"Sending data message for client_id={client_id}, fruit={final_fruit_item.fruit}, amount={final_fruit_item.amount} to exchange number {exchange_number}"
            )
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        self.amount_by_fruit_by_client.pop(client_id, None)
        self._broadcast_eof(client_id)

    def _broadcast_eof(self, client_id: str):
        logging.info(f"Broadcasting EOF message for client_id={client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _handle_input_eof(self, client_id: str):
        try:
            self.sum_control_publisher.send(
                message_protocol.internal.serialize([client_id])
            )
        finally:
            self._dispatch_eof_to_aggregations(client_id)

    def _process_data(self, client_id: str, fruit: str, amount: int):
        logging.info(
            f"Process data message: client_id={client_id}, fruit={fruit}, amount={amount}"
        )

        self.amount_by_fruit_by_client.setdefault(client_id, {})
        self.amount_by_fruit_by_client[client_id][fruit] = (
            self.amount_by_fruit_by_client[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            )
            + fruit_item.FruitItem(fruit, int(amount))
        )

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == MESSAGE_FIELDS:
            self._process_data(*fields)
        elif len(fields) == EOF_FIELDS:
            self._handle_input_eof(*fields)
        else:
            logging.error(f"Invalid message format: {fields}")
            nack()
            return
        ack()

    def process_control_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == EOF_FIELDS:
            self._dispatch_eof_to_aggregations(*fields)
        else:
            logging.error(f"Invalid control message format: {fields}")
            nack()
            return
        ack()

    def start(self):
        threads = [
            threading.Thread(
                target=self.input_queue.start_consuming,
                args=(self.process_data_messsage,),
            ),
            threading.Thread(
                target=self.sum_control_consumer.start_consuming,
                args=(self.process_control_messsage,),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

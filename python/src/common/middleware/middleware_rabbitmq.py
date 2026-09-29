import random
import string
from typing import Callable

import pika
from pika.channel import Channel
from pika.exceptions import AMQPConnectionError, AMQPError, ChannelWrongStateError
from pika.spec import Basic

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
)


# on_message_callback tiene como parámetros:
#   message - El valor tal y como lo recibe el método send de esta clase.
#   ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
#   nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
Callback = Callable[[bytes, Callable[[], None], Callable[[], None]], None]


class _RabbitMQConnection:
    """Clase interna para manejar la conexión a RabbitMQ."""

    def __init__(self, host: str):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e

    def _build_consumer_callback(self, on_message_callback: Callback):
        def consumer_callback(channel: Channel, method: Basic.Deliver, _, body: bytes):
            on_message_callback(
                body,
                lambda: channel.basic_ack(delivery_tag=method.delivery_tag),
                lambda: channel.basic_nack(delivery_tag=method.delivery_tag),
            )

        return consumer_callback

    def consume(self, queue_name: str, on_message_callback: Callback):
        try:
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._build_consumer_callback(on_message_callback),
            )
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e

    def publish(self, exchange: str, routing_key: str, message):
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except ChannelWrongStateError:
            return
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError from e


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host: str, queue_name: str):
        self._rabbitmq = _RabbitMQConnection(host)
        self.queue_name = queue_name

        try:
            self._rabbitmq.channel.queue_declare(queue=self.queue_name, durable=True)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e

    # Comienza a escuchar a la cola e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    #   message - El valor tal y como lo recibe el método send de esta clase.
    #   ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    #   nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback: Callback):
        self._rabbitmq.consume(self.queue_name, on_message_callback)

    # Si se estaba consumiendo desde la cola, se detiene la escucha. Si
    # no se estaba consumiendo de la cola, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        self._rabbitmq.stop_consuming()

    # Envía un mensaje a la cola con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message: str):
        self._rabbitmq.publish("", self.queue_name, message)

    # Se desconecta de la cola al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        self._rabbitmq.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host: str, exchange_name: str, routing_keys: list[str]):
        self._rabbitmq = _RabbitMQConnection(host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = self._build_queue_name(exchange_name)

        try:
            self._declare_resources()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError from e

    def _declare_resources(self):
        self._rabbitmq.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="direct",
            durable=True,
        )
        self._rabbitmq.channel.queue_declare(
            queue=self.queue_name,
            exclusive=True,
            auto_delete=True,
        )

        for routing_key in self.routing_keys:
            self._rabbitmq.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

    def _build_queue_name(self, exchange_name: str):
        suffix = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(12)
        )
        return f"{exchange_name}_{suffix}"

    # Comienza a escuchar al exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    #   message - El valor tal y como lo recibe el método send de esta clase.
    #   ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    #   nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback: Callback):
        self._rabbitmq.consume(self.queue_name, on_message_callback)

    # Si se estaba consumiendo desde el exchange, se detiene la escucha. Si
    # no se estaba consumiendo del exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        self._rabbitmq.stop_consuming()

    # Envía un mensaje al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        for routing_key in self.routing_keys:
            self._rabbitmq.publish(self.exchange_name, routing_key, message)

    # Se desconecta del exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        self._rabbitmq.close()

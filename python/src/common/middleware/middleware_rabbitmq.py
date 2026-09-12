import pika
import random
import string

from .middleware import (
    MessageMiddleware,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

DEFAULT_AMQP_PORT = 5672
DEFAULT_EXCHANGE_TYPE = "direct"
CONSUMER_PREFETCH_COUNT = 1

# Cubren todo lo que puede venir del transporte
_TRANSPORT_ERRORS = (
    pika.exceptions.AMQPError,
    pika.exceptions.ChannelError,
    pika.exceptions.ReentrancyError,
    OSError,
)


def _raise_domain_error(error):
    """Traduce una excepción del transporte a la excepción de dominio."""
    if isinstance(error, (pika.exceptions.AMQPConnectionError, OSError)):
        raise MessageMiddlewareDisconnectedError(str(error)) from error
    raise MessageMiddlewareMessageError(str(error)) from error


class _RabbitMQMiddleware(MessageMiddleware):
    """Contiene la lógica común a los dos modelos que son el ciclo de vida de la conexión y el canal.
    Cada instancia es dueña de una conexión y un canal propios. 
    """

    def __init__(self, host):
        self._connection = None
        self._channel = None
        
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, port=DEFAULT_AMQP_PORT))
            self._channel = self._connection.channel()
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

    def send(self, message):
        raise NotImplementedError("TODO")

    def start_consuming(self, on_message_callback):
        # pika entrega los mensajes con su propia firma de 4 args, mientras que la interfaz espera 3. 
        # dispatch funciona como un adaptador entre las 2, pasa el body tal cual y arma los 2 callables de confirmación.
        def _dispatch(channel, method, props, body):
            on_message_callback(body,
                                lambda: channel.basic_ack(method.delivery_tag),
                                lambda: channel.basic_nack(method.delivery_tag, requeue=True))

        try:
            self._channel.basic_qos(prefetch_count=CONSUMER_PREFETCH_COUNT)
            self._channel.basic_consume(queue=self._queue_name,
                                        on_message_callback=_dispatch,
                                        auto_ack=False)
            self._channel.start_consuming()
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except _TRANSPORT_ERRORS as error: 
            _raise_domain_error(error)

    def close(self):
        try:
            try:
                if self._channel is not None and self._channel.is_open:
                    self._channel.close()
            finally:
                # Garantizamos que la conexión se cierre aunque falle el cierre del canal, asi evitamos un socket colgado
                if self._connection is not None and self._connection.is_open:
                    self._connection.close()
        except _TRANSPORT_ERRORS as error:
            raise MessageMiddlewareCloseError(str(error)) from error


class MessageMiddlewareQueueRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareQueue):
    """Hereda la implementación de _RabbitMQMiddleware y el contrato de MessageMiddlewareQueue."""

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name

        # La cola se declara en el constructor, tanto para el productor como para el consumidor
        try:
            self._channel.queue_declare(
                queue=queue_name,
                durable=False,
                exclusive=False,
                auto_delete=False)
        except _TRANSPORT_ERRORS as error:
            self.close()
            _raise_domain_error(error)

    def send(self, message):
        try:
            self._channel.basic_publish(exchange="",
                                        routing_key=self._queue_name, 
                                        body=message)
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareExchange):
    """Hereda la implementación de _RabbitMQMiddleware y el contrato de MessageMiddlewareQueue."""

    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys)

        try:
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=DEFAULT_EXCHANGE_TYPE,
                durable=False)
        except _TRANSPORT_ERRORS as error:
            self.close()
            _raise_domain_error(error)

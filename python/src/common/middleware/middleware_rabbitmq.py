import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddleware, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

DEFAULT_AMQP_PORT = 5672
DEFAULT_EXCHANGE_TYPE = "direct"

_TRANSPORT_ERRORS = (
    pika.exceptions.AMQPError,
    pika.exceptions.ChannelError,
    pika.exceptions.ReentrancyError,
    OSError,
)

def _raise_domain_error(error):
    """Traduce una excepción del transporte a la excepción de dominio"""
    if isinstance(error, (pika.exceptions.AMQPConnectionError, OSError)):
        raise MessageMiddlewareDisconnectedError(str(error)) from error
    raise MessageMiddlewareMessageError(str(error)) from error

class _RabbitMQMiddleware(MessageMiddleware):
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
        raise NotImplementedError("TODO")

    def stop_consuming(self):
        raise NotImplementedError("TODO")

    def close(self):
        try: 
            try: 
                if self._channel is not None and self._channel.is_open:
                    self._channel.close()
            finally:
                if self._connection is not None and self._connection.is_open:
                    self._connection.close()
        except _TRANSPORT_ERRORS as error:
            raise MessageMiddlewareCloseError(str(error)) from error

class MessageMiddlewareQueueRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name

        try:
            self._channel.queue_declare(
                queue=queue_name,
                durable=False,
                exclusive=False,
                auto_delete=False)
        except _TRANSPORT_ERRORS as error:
            _raise_domain_error(error)

class MessageMiddlewareExchangeRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareExchange):
    
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
            _raise_domain_error(error)

"""
Generic RabbitMQ consumer factory.
Wraps aio_pika queue setup so individual layers only need to supply a
callback and a routing key.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable

import aio_pika

from config.settings import EXCHANGE_MAIN, RABBITMQ_URL

logger = logging.getLogger(__name__)

MessageCallback = Callable[[aio_pika.IncomingMessage], Awaitable[None]]


async def create_consumer(
    queue_name: str,
    routing_key: str,
    callback: MessageCallback,
    prefetch: int = 10,
    url: str = RABBITMQ_URL,
    exchange_name: str = EXCHANGE_MAIN,
) -> tuple[aio_pika.abc.AbstractRobustConnection, aio_pika.abc.AbstractQueue]:
    """
    Declare exchange + queue, bind routing key, and start consuming.

    Returns
    -------
    (connection, queue)  – caller is responsible for closing the connection.
    """
    connection = await aio_pika.connect_robust(url)
    channel    = await connection.channel()
    await channel.set_qos(prefetch_count=prefetch)

    exchange = await channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(exchange, routing_key=routing_key)

    await queue.consume(callback)
    logger.info("Consumer listening on queue '%s' (key='%s')", queue_name, routing_key)
    return connection, queue

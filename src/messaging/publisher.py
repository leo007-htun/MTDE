"""
Generic RabbitMQ publisher.
Provides a thin async wrapper around aio_pika for use by the control loop
and any component that needs to inject messages outside of a consumer context
(e.g. the sensor simulator, market signal injector).
"""
from __future__ import annotations

import logging
from typing import Any

import aio_pika
from pydantic import BaseModel

from config.settings import EXCHANGE_MAIN, RABBITMQ_URL

logger = logging.getLogger(__name__)


class AsyncPublisher:
    """
    Async RabbitMQ publisher that maintains a single persistent connection
    and channel.  Call connect() before first use, close() on shutdown.
    """

    def __init__(self, url: str = RABBITMQ_URL, exchange: str = EXCHANGE_MAIN) -> None:
        self._url       = url
        self._exchange_name = exchange
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel:    aio_pika.abc.AbstractChannel | None = None
        self._exchange:   aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel    = await self._connection.channel()
        self._exchange   = await self._channel.declare_exchange(
            self._exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        logger.info("Publisher connected to exchange '%s'", self._exchange_name)

    async def publish(
        self,
        payload: BaseModel | dict | bytes,
        routing_key: str,
        persistent: bool = True,
    ) -> None:
        """
        Publish a message.

        Parameters
        ----------
        payload     : Pydantic model, dict, or raw bytes
        routing_key : AMQP topic routing key (e.g. 'iot.sensor')
        persistent  : delivery mode PERSISTENT if True
        """
        if isinstance(payload, BaseModel):
            body = payload.model_dump_json().encode()
        elif isinstance(payload, dict):
            import json
            body = json.dumps(payload).encode()
        else:
            body = payload  # already bytes

        delivery_mode = (
            aio_pika.DeliveryMode.PERSISTENT if persistent
            else aio_pika.DeliveryMode.NOT_PERSISTENT
        )

        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=delivery_mode,
            ),
            routing_key=routing_key,
        )
        logger.debug("Published %d bytes → %s", len(body), routing_key)

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
            logger.info("Publisher connection closed")

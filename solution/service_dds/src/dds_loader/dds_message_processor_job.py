from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    LOAD_SRC = "orders-system-kafka"

    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        processed = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            if msg.get("object_type") != "order":
                continue

            order = msg["payload"]
            saved = self._dds_repository.save_order(order, datetime.utcnow(), self.LOAD_SRC)

            # Выходное сообщение для сервиса CDM: всё, что нужно для счётчиков витрин.
            dst_msg = {
                "object_id": order["id"],
                "object_type": "order_report",
                "payload": {
                    "id": order["id"],
                    "order_dt": order["date"],
                    "status": order["status"],
                    "user_id": str(saved["h_user_pk"]),
                    "products": saved["products"],
                }
            }
            self._producer.produce(dst_msg)
            processed += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH, processed {processed} messages")

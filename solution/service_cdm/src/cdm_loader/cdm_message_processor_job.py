import uuid
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        processed = 0
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            if msg.get("object_type") != "order_report":
                continue

            payload = msg["payload"]
            self._cdm_repository.refresh_user_counters(uuid.UUID(payload["user_id"]))
            processed += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH, processed {processed} messages")

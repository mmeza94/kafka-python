from kafka import KafkaConsumer,KafkaProducer
from kafka.admin import KafkaAdminClient
import contextlib
import json
import xmltodict
from lib.kafka_schemas import KafkaConfigurations,KafkaClient
from lib.serializers import SerializerRegistry



class KafkaConnection:
    def __init__(self,config:KafkaConfigurations) -> None:
        self.config:KafkaConfigurations = config
        self.serializer_registry:SerializerRegistry = SerializerRegistry()

    def _get_consumer(self):
        return KafkaConsumer()

    def _get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            value_serializer=self.serializer_registry.get_serializer(self.config.value_serializer)
        )

    def _get_admin(self):
        return KafkaAdminClient(
            bootstrap_servers=self.config.bootstrap_servers
        )

    @contextlib.contextmanager
    def  initialize(self, kafka_client:KafkaClient):
        try:
            print(f"Initializing client {kafka_client}")
            if kafka_client == KafkaClient.producer:
                client = self._get_producer()
            elif kafka_client == KafkaClient.consumer:
                client = self._get_consumer()
            elif kafka_client == KafkaClient.admin:
                client = self._get_admin()
            else:
                raise ValueError(f"Unknown Kafka client type: {kafka_client}")
            yield client
        except Exception as ex:
            raise 
        finally:
            client.close()

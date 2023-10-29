from kafka import KafkaConsumer,KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from dataclasses import dataclass
from enum import Enum
import contextlib
import json






@dataclass
class KafkaConfigurations:
    bootstrap_servers:str=None
    value_serializer:object=None
    value_deserializer:object=None
    topic:str=None
    num_partitions:int=1
    on_success:bool=False
    on_failure:bool=False







class KafkaClient(Enum):
    producer = 1
    consumer = 2
    admin = 3








class KafkaConnection:
    def __init__(self,config:KafkaConfigurations) -> None:
        self.config:KafkaConfigurations = config
        self.serializers = {
            "utf-8":lambda x : json.dumps(x).encode("utf-8")
        }

    def _get_consumer(self):
        return KafkaConsumer()

    def _get_producer(self):
        serializer = self.serializers[self.config.value_serializer]
        return KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            value_serializer=serializer
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
        finally:
            client.close()




class Message:
    kafka_success = "SUCCESS!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    kafka_failure = "ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"






class KafkaDataAccess:
    def __init__(self,config:KafkaConfigurations) -> None:
        self.config:KafkaConfigurations = config
        self.connection:KafkaConnection = KafkaConnection(config)


    def _on_success(self,metadata):
        if self.config.on_success:
            print(Message.kafka_success)
            print(metadata)

    def _on_failure(self,error):
        if self.config.on_failure:
            print(Message.kafka_failure)
            print(error)

    def send_messages(self,messages:list[dict]):
        with self.connection.initialize(KafkaClient.producer) as client:
            for message in messages:
                client.send(value=message,topic=self.config.topic)\
                      .add_callback(self._on_success)\
                      .add_errback(self._on_failure)

    def read_topic(self):
        with self.connection.initialize(KafkaClient.consumer) as client:
            pass

    def create_topics(self,topic_name,num_partitions=1,replication_factor=1):
        with self.connection.initialize(KafkaClient.admin) as client:
            topic = NewTopic(name=topic_name,num_partitions=num_partitions,replication_factor=replication_factor)
            client.create_topics([topic])

    def delete_topics(self,topic):
        with self.connection.initialize(KafkaClient.admin) as client:
            client.delete_topics([topic])



class ConfigurationsManager:
    def  __init__(self,debug=False) -> None:
        self.debug = debug
        self.config = self._get_config()
        self.kafka_config:KafkaConfigurations = self._get_kafka_config()


    def _get_local_config(self):
        print("Loading config form local path")
        config = json.load(open("/workspaces/kafka-python/config.json"))
        return config

    def _get_config(self):
        if self.debug:
            config = self._get_local_config()
        return config

    def _get_kafka_config(self):
        return KafkaConfigurations(**self.config["kafka"])

# message = {
#     "Id":777,
#     "product":"Iphone17"
# }
message2 = {
    "Id":888,
    "product":"Iphone1"
}
message3 = {
    "Id":999,
    "product":"Iphone3"
}



config_manager = ConfigurationsManager(debug=True)
kafka = KafkaDataAccess(config_manager.kafka_config)
kafka.send_messages([message2,message3])
# kafka.create_topics(topic_name="tpc-test")



























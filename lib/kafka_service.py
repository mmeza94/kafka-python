from lib.kafka_schemas import Message,KafkaConfigurations,KafkaClient,Topic,Record
from lib.kafka_connection import KafkaConnection
from kafka.admin import NewTopic, ConfigResource
from kafka.admin.config_resource import ConfigResourceType


class KafkaService:
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

    def send_messages(self,records:list[Record]):
        with self.connection.initialize(KafkaClient.producer) as client:
            for record in records:
                client.send(value=record.value,topic=record.topic)\
                      .add_callback(self._on_success)\
                      .add_errback(self._on_failure)

    def read_topic(self):
        with self.connection.initialize(KafkaClient.consumer) as client:
            pass

    def create_topics(self,topic_props:list[Topic]):
        with self.connection.initialize(KafkaClient.admin) as client:
            for topic in topic_props:
                topic = NewTopic(**topic.to_dict())
                client.create_topics([topic])

    def delete_topics(self,topic_names:list[str]):
        with self.connection.initialize(KafkaClient.admin) as client:
            for topic in topic_names:
                client.delete_topics([topic])

    def describe_topic(self,topic_name:str):
        with self.connection.initialize(KafkaClient.admin) as client:
            return client.describe_topics(topics=[topic_name])
                   
    def list_topics(self):
        with self.connection.initialize(KafkaClient.admin) as client:
            return client.list_topics()
    
    def list_consumer_groups(self):
        with self.connection.initialize(KafkaClient.admin) as client:
            return client.list_consumer_groups()
        
    def topics_exists(self,topic:str):
        topics = self.list_topics()
        return topic in topics
        
    def describe_resource_configurations(self,resource_type,resource_name):
        with self.connection.initialize(KafkaClient.admin) as client:
            if resource_type == "topic":
                config_resource = ConfigResource(ConfigResourceType.TOPIC, resource_name)
            elif resource_type == "broker":
                config_resource = ConfigResource(ConfigResourceType.BROKER, resource_name)
            else:
                raise ValueError(f"Unsupported type {resource_type}")
            return client.describe_configs([config_resource])





















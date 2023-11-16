
from dataclasses import dataclass,asdict
from enum import Enum


@dataclass
class Configurations:
    def to_dict(self):
        return asdict(self)

@dataclass
class KafkaConfigurations(Configurations):
    bootstrap_servers:str
    value_serializer:object=None
    value_deserializer:object=None
    on_success:bool=False
    on_failure:bool=False
    enable_auto_commit:bool=True
    auto_offset_reset:str=None
    group_id:str=None
    topic_name:str=None


@dataclass
class Topic(Configurations):
    name:str
    num_partitions:int=1
    replication_factor:int=1


@dataclass
class Record(Configurations):
    value:object
    topic:str
    headers:list[tuple]=None
    key:str=None



class KafkaClient(Enum):
    producer = 1
    consumer = 2
    admin = 3



class Message:
    kafka_success = "Succesful execution"
    kafka_failure = "Execution Failed"













from lib.kafka_service import KafkaService
from lib.kafka_schemas import Record
from configurations_manager import ConfigurationsManager

conf_manager = ConfigurationsManager(debug=True)
kafka = KafkaService(conf_manager.kafka_config)

def test_list_topics():
    topics = kafka.list_topics()
    print("topics retrieved succesfully...")
    print(topics)

def test_describe_topics():
    topic = "tpc-test"
    description = kafka.describe_topic(topic)
    print(description)

def test_describe_conf():
    print(kafka.describe_resource_configurations("topic","tpc-test"))

def test_consumer_groups():
    print(kafka.list_consumer_groups())

def test_send_message():
    xml = {
        "root":{
             "QWEQ":"ABC",
            "ASDA":"DFG"
        } 
    }
    rec = Record(value=xml,topic="tpc-test")
    kafka.send_messages([rec])

test_send_message()









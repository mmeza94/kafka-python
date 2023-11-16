from lib.kafka_service import KafkaService
from lib.kafka_schemas import Record
from configurations_manager import ConfigurationsManager

conf_manager = ConfigurationsManager(debug=True)
kafka = KafkaService(conf_manager.kafka_config)



import random
import string
import uuid

def generate_random_id():
    return str(uuid.uuid4())

def generate_random_string(length=10):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

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
    messages = []
    for i in range(10):
        m = {
            "ID":generate_random_id(),
            "Content":generate_random_string()
        }
        rec = Record(value=m,topic="tpc-test")
        messages.append(rec)
    kafka.send_messages(messages)

def test_read_message():
    m =  kafka.read_topic(10)
    for i in m:
        print(i)

test_read_message()






























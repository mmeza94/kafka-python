import json

from lib.kafka_schemas import KafkaConfigurations


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
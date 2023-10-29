import json
import xmltodict

class SerializerRegistry:
    def __init__(self) -> None:
        self._serializer = {
            "json_utf8":lambda message : json.dumps(message).encode("utf-8"),
            "xml_utf8": lambda message: xmltodict.unparse(message).encode("utf-8")
        }
        self._deserializer = {
            "json_utf8":lambda message : json.loads(message.decode("utf-8")),
            "xml_utf8": lambda message: xmltodict.parse(message.decode("utf-8"))
        }

    def get_serializer(self,code):
        return self._serializer[code]

    def get_deserializer(self,code):
        return self._deserializer[code]










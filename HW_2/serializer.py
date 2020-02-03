import json
from abc import abstractmethod

from constants.constants import Constants


class Serializer:

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def serialize(self, dictionary: dict) -> str:
        pass

    @abstractmethod
    def deserialize(self, serialized_string: str) -> dict:
        pass


class JsonSerializer(Serializer):

    @property
    def name(self) -> str:
        return Constants.JSON_SERIALIZER_NAME

    def serialize(self, dictionary: dict) -> str:
        return json.dumps(dictionary)

    def deserialize(self, serialized_string: str) -> dict:
        return json.loads(serialized_string)

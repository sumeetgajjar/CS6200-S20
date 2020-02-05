import json
import pickle
from abc import abstractmethod

from constants.constants import Constants


class Serializer:

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def serialize(self, obj_to_serialize) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, bytes_to_deserialize: bytes):
        pass


class JsonSerializer(Serializer):

    @property
    def name(self) -> str:
        return Constants.JSON_SERIALIZER_NAME

    def serialize(self, obj_to_serialize) -> bytes:
        return json.dumps(obj_to_serialize).encode(Constants.SERIALIZER_ENCODING)

    def deserialize(self, bytes_to_deserialize: bytes):
        return json.loads(bytes_to_deserialize.decode(Constants.SERIALIZER_ENCODING))


class PickleSerializer(Serializer):

    @property
    def name(self) -> str:
        return Constants.PICKLE_SERIALIZER_NAME

    def serialize(self, obj_to_serialize) -> bytes:
        return pickle.dumps(obj_to_serialize, protocol=Constants.PICKLE_PROTOCOL)

    def deserialize(self, bytes_to_deserialize: bytes):
        return pickle.loads(bytes_to_deserialize)

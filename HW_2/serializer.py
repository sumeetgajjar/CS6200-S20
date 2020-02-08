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


class TermvectorSerializer(Serializer):
    _TERMVECTOR_SEPARATOR = '!'
    _TF_INFO_SEPARATOR = ','
    _POSITION_SEPARATOR = '@'

    @property
    def name(self) -> str:
        return Constants.TERMVECTOR_SERIALIZER_NAME

    def serialize(self, termvector) -> bytes:
        """
        <ttf>[_TERMVECTOR_SEPARATOR]<doc_id[_TF_INFO_SEPARATOR]tf[_TF_INFO_SEPARATOR]p1[_POSITION_SEPARATOR]p2[_POSITION_SEPARATOR]>[_TERMVECTOR_SEPARATOR]<doc_id[_TF_INFO_SEPARATOR]tf[_TF_INFO_SEPARATOR]p1[_POSITION_SEPARATOR]p2[_POSITION_SEPARATOR]>
        :param termvector:
        :return:
        """
        str_list = [str(termvector['ttf'])]
        for doc_id, tf_info in termvector['tf'].items():
            positions_str = self._POSITION_SEPARATOR.join(map(str, tf_info['pos']))
            tf_info_str = self._TF_INFO_SEPARATOR.join([str(doc_id), str(tf_info['tf']), positions_str])
            str_list.append(tf_info_str)

        serialized_str = self._TERMVECTOR_SEPARATOR.join(str_list)
        return serialized_str.encode(Constants.SERIALIZER_ENCODING)

    def deserialize(self, bytes_to_deserialize: bytes):
        termvector = {}
        deserialized_str = bytes_to_deserialize.decode(Constants.SERIALIZER_ENCODING)
        termvector_splits = deserialized_str.split(self._TERMVECTOR_SEPARATOR)
        termvector['ttf'] = int(termvector_splits[0])
        termvector['tf'] = {}

        for termvector_split in termvector_splits[1:]:
            tf_info_splits = termvector_split.split(self._TF_INFO_SEPARATOR)
            doc_id = tf_info_splits[0]
            tf = int(tf_info_splits[1])
            positions = list(map(int, tf_info_splits[2].split(self._POSITION_SEPARATOR)))
            termvector['tf'][doc_id] = {'tf': tf, 'pos': positions}

        return termvector

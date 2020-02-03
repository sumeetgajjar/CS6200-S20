import uuid

from HW_2.compressor import Compressor
from HW_2.serializer import Serializer
from constants.constants import Constants
from utils.decorators import timing
from utils.utils import Utils


class CustomIndex:
    def __init__(self, tokenizer, stopwords_filter, stemmer, compressor: Compressor, serializer: Serializer) -> None:
        self.tokenizer = tokenizer
        self.stopwords_filter = stopwords_filter
        self.stemmer = stemmer
        self.compressor = compressor
        self.serializer = serializer

    @classmethod
    def _calculate_and_update_tf_info(cls, document_id, tokens, tf_info):
        for token in tokens:
            term = token[0]
            if term not in tf_info:
                tf_info[term] = {'ttf': 0, 'tf': {}}

            # updating the ttf
            tf_info[term]['ttf'] += 1

            term_tf_info = tf_info[term]['tf']
            if document_id not in term:
                term_tf_info[document_id] = {'tf': 0, 'pos': []}

            termvector = term_tf_info[document_id]
            # updating the tf
            termvector['tf'] += 1

            # updating the position information
            termvector['pos'].append(token[1])

    @classmethod
    def _create_tf_info_batches(cls, tf_info: dict):
        i = 0
        tf_info_batch = {}
        for term, info in tf_info.items():
            tf_info_batch[term] = info
            i += 1

            if i % Constants.TERMS_IN_SINGLE_WRITE == 0:
                temp = tf_info_batch
                tf_info_batch = {}
                yield temp

        if tf_info_batch:
            yield tf_info_batch

    @classmethod
    def _get_index_file_path(cls):
        return '{}/{}/{}/{}.txt'.format(Utils.get_data_dir_abs_path(), 'custom-index', 'index', uuid.uuid4())

    @classmethod
    def _get_catalog_file_path(cls):
        return '{}/{}/{}/{}.txt'.format(Utils.get_data_dir_abs_path(), 'custom-index', 'catalog', uuid.uuid4())

    def _write_tf_info_to_index_file(self, tf_info):
        tf_info_metadata = {}
        index_file_path = self._get_index_file_path()

        with open(index_file_path, 'wb') as file:
            for batch in self._create_tf_info_batches(tf_info):
                current_pos = file.tell()

                serialized_tf_info = self.serializer.serialize(batch)
                compressed_tf_info_bytes = self.compressor.compress_string_to_bytes(serialized_tf_info)

                size = file.write(compressed_tf_info_bytes)
                file.write(b"\n")

                # optimization since all the terms in the same batch will have same pos and size
                temp = {'pos': current_pos, 'size': size}
                for term in batch.keys():
                    tf_info_metadata[term] = temp

        return tf_info_metadata, index_file_path

    def _write_catalog_to_file(self, catalog):
        catalog_file_path = self._get_catalog_file_path()
        with open(catalog_file_path, 'wb') as file:
            serialized_catalog = self.serializer.serialize(catalog)
            compressed_catalog_bytes = self.compressor.compress_string_to_bytes(serialized_catalog)
            file.write(compressed_catalog_bytes)

        return catalog_file_path

    def _read_catalog_to_file(self, catalog_file_path):
        with open(catalog_file_path, 'rb') as file:
            compressed_catalog_bytes = file.read()
            serialized_catalog = self.compressor.decompress_bytes_to_string(compressed_catalog_bytes)
            catalog = self.serializer.deserialize(serialized_catalog)

        return catalog

    def _create_catalog(self, tf_info_metadata, index_file_path, ):
        catalog = {
            'metadata': {
                'index_file_path': index_file_path,
                'compressor': self.compressor.name,
                'serializer': self.serializer.name
            },
            'tf_info_metadata': tf_info_metadata
        }
        return catalog

    @timing
    def add_documents(self, documents, index_head, enable_stemming):

        tf_info = {}
        for document in documents:
            tokens = self.tokenizer.tokenize(document['text'])

            if index_head:
                head_tokens = self.tokenizer.tokenize(document['head'])
                tokens.extend(head_tokens)

            tokens = self.stopwords_filter.filter(tokens)

            if enable_stemming:
                tokens = [self.stemmer.stem(token) for token in tokens]

            self._calculate_and_update_tf_info(document['id'], tokens, tf_info)

        tf_info_metadata, index_file_path = self._write_tf_info_to_index_file(tf_info)
        catalog = self._create_catalog(tf_info_metadata, index_file_path)
        catalog_file_path = self._write_catalog_to_file(catalog)
        return catalog_file_path, index_file_path

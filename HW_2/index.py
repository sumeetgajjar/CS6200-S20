import json
import uuid

from constants.constants import Constants
from utils.utils import Utils


class CustomIndex:
    def __init__(self, tokenizer, stopwords_filter, stemmer) -> None:
        self.tokenizer = tokenizer
        self.stopwords_filter = stopwords_filter
        self.stemmer = stemmer

    @classmethod
    def _calculate_and_update_tf_info(cls, document_id, tokens, tf_info):
        for token in tokens:
            term = token[0]
            if term not in tf_info:
                tf_info[term] = {'ttf': 0, 'tf': {}}

            term_tf_info = tf_info[term]['tf']
            if document_id not in term:
                term_tf_info[document_id] = {'tf': 0, 'pos': []}

            termvector = term_tf_info[document_id]
            termvector['tf'] += 1
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
        return '{}/{}/{}/{}'.format(Utils.get_ap_data_path(), 'custom-index', 'index', uuid.uuid4())

    @classmethod
    def _get_catalog_file_path(cls):
        return '{}/{}/{}/{}'.format(Utils.get_ap_data_path(), 'custom-index', 'catalog', uuid.uuid4())

    @classmethod
    def _convert_dict_to_bytes(cls, tf_info):
        return bytearray(json.dumps(tf_info), encoding=Constants.AP_DATA_FILE_ENCODING)

    def _write_tf_info_to_index_file(self, tf_info):
        catalog = {}
        index_file_path = self._get_index_file_path()

        with open(index_file_path, 'wb') as file:
            for batch in self._create_tf_info_batches(tf_info):
                current_pos = file.tell()
                size = file.write(self._convert_dict_to_bytes(batch))

                temp = {'pos': current_pos, 'size': size}
                for term in batch.keys():
                    catalog[term] = temp

        return catalog, index_file_path

    def _write_catalog_to_file(self, catalog):
        catalog_file_path = self._get_catalog_file_path()
        with open(catalog_file_path, 'w') as file:
            file.write(json.dumps(catalog))

        return catalog_file_path

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

        catalog, index_file_path = self._write_tf_info_to_index_file(tf_info)
        catalog_file_path = self._write_catalog_to_file(catalog)
        return catalog_file_path, index_file_path

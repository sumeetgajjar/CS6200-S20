import datetime
import json
import logging
import os
from functools import lru_cache

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

        self.catalog = None
        self.metadata = None
        self.index_file_handle = None
        self.document_length = None

        self._create_dirs_if_absent()

    def _create_dirs_if_absent(self):
        for path in [self._get_metadata_dir(), self._get_index_data_dir(), self._get_catalog_data_dir()]:
            if not os.path.isdir(path):
                os.makedirs(path)

    @classmethod
    def _calculate_and_update_termvectors(cls, document_id, tokens, termvectors):
        for token in tokens:
            term = token[0]
            if term not in termvectors:
                termvectors[term] = {'ttf': 0, 'tf': {}}

            # updating the ttf
            termvectors[term]['ttf'] += 1

            tf_info_dict = termvectors[term]['tf']
            if document_id not in tf_info_dict:
                tf_info_dict[document_id] = {'tf': 0, 'pos': []}

            tf_info = tf_info_dict[document_id]
            # updating the tf
            tf_info['tf'] += 1

            # updating the position information
            tf_info['pos'].append(token[1])

    @classmethod
    def _merge_termvectors(cls, termvector_1, termvector_2):
        # Merging the ttf
        merged_termvector = {
            'ttf': termvector_1['ttf'] + termvector_2['ttf'],
            'tf': {}
        }

        # Merging the tf and positions
        merged_term_tf_info_dict = merged_termvector['tf']
        for document_id, tf_info_1 in termvector_1['tf'].items():
            if document_id not in merged_term_tf_info_dict:
                merged_term_tf_info_dict[document_id] = {'tf': 0, 'pos': []}

            merged_tf_info = merged_term_tf_info_dict[document_id]
            merged_tf_info['tf'] += tf_info_1['tf']
            merged_tf_info['pos'].extend(tf_info_1['pos'])

            tf_info_2 = termvector_2['tf'].get(document_id)
            if tf_info_2:
                merged_tf_info['tf'] += tf_info_2['tf']
                merged_tf_info['pos'].extend(tf_info_2['pos'])

        for document_id, tf_info_2 in termvector_2['tf'].items():
            if document_id not in termvector_1['tf']:
                merged_term_tf_info_dict[document_id] = {'tf': 0, 'pos': []}
                merged_term_tf_info_dict[document_id]['tf'] += tf_info_2['tf']
                merged_term_tf_info_dict[document_id]['pos'].extend(tf_info_2['pos'])

        return merged_termvector

    @classmethod
    def _get_custom_index_dir(cls):
        return '{}/{}'.format(Utils.get_data_dir_abs_path(), 'custom-index')

    def _get_index_data_dir(self):
        return '{}/{}/{}'.format(self._get_custom_index_dir(), 'data', 'index')

    def _get_new_index_file_path(self):
        return '{}/{}.txt'.format(self._get_index_data_dir(), Utils.get_random_file_name_with_ts())

    def _get_catalog_data_dir(self):
        return '{}/{}/{}'.format(self._get_custom_index_dir(), 'data', 'catalog')

    def _get_new_catalog_file_path(self):
        return '{}/{}.txt'.format(self._get_catalog_data_dir(), Utils.get_random_file_name_with_ts())

    def _get_metadata_dir(self):
        return '{}/{}'.format(self._get_custom_index_dir(), 'metadata')

    def _get_new_metadata_file_path(self):
        return '{}/{}.txt'.format(self._get_metadata_dir(), Utils.get_random_file_name_with_ts())

    def _write_termvectors_to_index_file(self, termvectors):
        catalog_data = {}
        index_file_path = self._get_new_index_file_path()

        with open(index_file_path, 'wb') as file:
            for term, termvector in termvectors.items():
                current_pos = file.tell()
                size = self._write_termvector(file, termvector)

                file.write(b"\n")

                catalog_data[term] = {'pos': current_pos, 'size': size}

        return catalog_data, index_file_path

    def _write_catalog_to_file(self, catalog):
        catalog_file_path = self._get_new_catalog_file_path()
        with open(catalog_file_path, 'wb') as file:
            catalog_bytes = json.dumps(catalog).encode(Constants.AP_DATA_FILE_ENCODING)
            self._write_bytes(file, catalog_bytes)

        return catalog_file_path

    def _read_catalog_to_file(self, catalog_file_path):
        with open(catalog_file_path, 'rb') as file:
            catalog_bytes = self._read_bytes(file)
            catalog = json.loads(catalog_bytes.decode(Constants.AP_DATA_FILE_ENCODING))

        return catalog

    @classmethod
    def _read_metadata_from_file(cls, metadata_file_path):
        with open(metadata_file_path, 'r') as file:
            metadata = json.load(file)

        return metadata

    def _write_metadata_to_file(self, metadata):
        metadata_file_path = self._get_new_metadata_file_path()
        logging.info('Metadata path:{}'.format(metadata_file_path))
        with open(metadata_file_path, 'w') as file:
            json.dump(metadata, file, indent=True)
        return metadata_file_path

    def _create_metadata(self, catalog_file_path, index_file_path):
        return {
            'index_file_path': index_file_path,
            'catalog_file_path': catalog_file_path,
            'tokenizer': self.tokenizer.name,
            'stopwords_filter': self.stopwords_filter.name,
            'stemmer': self.stemmer.name,
            'serializer': self.serializer.name,
            'compressor': self.compressor.name,
            'timestamp': str(datetime.datetime.now())
        }

    def _create_documents_index_and_catalog(self, documents, index_head, enable_stemming):

        termvectors = {}
        for document in documents:

            tokens = self.analyze(document['text'], enable_stemming)
            if index_head:
                head_tokens = self.analyze(document.get('head', ''), enable_stemming)
                tokens.extend(head_tokens)

            self._calculate_and_update_termvectors(document['id'], tokens, termvectors)

        catalog_data, index_file_path = self._write_termvectors_to_index_file(termvectors)
        catalog = {
            'metadata': {
                'total_docs': len(documents)
            },
            'data': catalog_data
        }
        catalog_file_path = self._write_catalog_to_file(catalog)
        metadata = self._create_metadata(catalog_file_path, index_file_path)
        return metadata

    def _read_bytes(self, file, start=0, size=-1):
        file.seek(start)
        compressed_bytes = file.read(size)
        decompressed_bytes = self.compressor.decompress_bytes(compressed_bytes)
        return decompressed_bytes

    def _read_termvector(self, file, start, size):
        termvector_bytes = self._read_bytes(file, start, size)
        return self.serializer.deserialize(termvector_bytes)

    def _write_bytes(self, file, bytes_to_write):
        compressed_bytes = self.compressor.compress_bytes(bytes_to_write)
        return file.write(compressed_bytes)

    def _write_termvector(self, file, termvector):
        serialized_bytes = self.serializer.serialize(termvector)
        return self._write_bytes(file, serialized_bytes)

    @classmethod
    def _delete_index_and_catalog_files(cls, metadata):
        os.remove(metadata['index_file_path'])
        os.remove(metadata['catalog_file_path'])

    @classmethod
    def _delete_catalog_file(cls, metadata):
        os.remove(metadata['catalog_file_path'])

    def _merge_2_index_and_catalog(self, metadata_list):
        metadata_1 = metadata_list[0]
        catalog_1 = self._read_catalog_to_file(metadata_1['catalog_file_path'])

        if len(metadata_list) == 1:
            self._delete_catalog_file(metadata_1)
            return catalog_1, metadata_1['index_file_path']

        metadata_2 = metadata_list[1]
        catalog_2 = self._read_catalog_to_file(metadata_2['catalog_file_path'])

        merged_index_path = self._get_new_index_file_path()
        merged_catalog_data = {}
        with open(merged_index_path, 'wb') as merged_index_file, \
                open(metadata_1['index_file_path'], 'rb') as index_file_1, \
                open(metadata_2['index_file_path'], 'rb') as index_file_2:

            for term, read_metadata_1 in catalog_1['data'].items():
                termvector_1 = self._read_termvector(index_file_1, read_metadata_1['pos'], read_metadata_1['size'])
                if term in catalog_2['data']:
                    read_metadata_2 = catalog_2['data'][term]
                    termvector_2 = self._read_termvector(index_file_2, read_metadata_2['pos'], read_metadata_2['size'])
                    merged_termvector = self._merge_termvectors(termvector_1, termvector_2)
                else:
                    merged_termvector = termvector_1

                pos = merged_index_file.tell()
                size = self._write_termvector(merged_index_file, merged_termvector)
                merged_index_file.write(b'\n')

                merged_catalog_data[term] = {'pos': pos, 'size': size}

            for term, read_metadata_2 in catalog_2['data'].items():
                if term not in catalog_1['data']:
                    termvector_2 = self._read_termvector(index_file_2, read_metadata_2['pos'], read_metadata_2['size'])

                    pos = merged_index_file.tell()
                    size = self._write_termvector(merged_index_file, termvector_2)
                    merged_index_file.write(b'\n')

                    merged_catalog_data[term] = {'pos': pos, 'size': size}

        self._delete_index_and_catalog_files(metadata_1)
        self._delete_index_and_catalog_files(metadata_2)

        merged_catalog = {
            'metadata': {
                'total_docs': catalog_1['metadata']['total_docs'] + catalog_2['metadata']['total_docs']
            },
            'data': merged_catalog_data
        }
        return merged_catalog, merged_index_path

    @classmethod
    def _make_files_readonly(cls, metadata_file_path, metadata):
        for file in [metadata_file_path, metadata['catalog_file_path'], metadata['index_file_path']]:
            os.chmod(file, 0o444)

    def _merge_indexes_and_catalogs(self, metadata_list: list):

        while len(metadata_list) > 1:
            logging.info("Metadata list size: {}".format(len(metadata_list)))
            tasks = list(Utils.split_list_into_sub_lists(metadata_list, sub_list_size=2))
            results = Utils.run_tasks_parallelly(self._merge_2_index_and_catalog, tasks, 8)
            metadata_list = []
            for merged_catalog, merged_index_path in results:
                merged_catalog_file_path = self._write_catalog_to_file(merged_catalog)
                merged_metadata = self._create_metadata(merged_catalog_file_path, merged_index_path)
                metadata_list.append(merged_metadata)

        if len(metadata_list) != 1:
            raise RuntimeError("More than 1 metadata after merging")

        merged_metadata = metadata_list[0]
        return merged_metadata

    @timing
    def _compute_document_length(self):
        document_length = {}

        for term in self.catalog['data'].keys():
            termvector = self.get_termvector(term)
            for doc_id, tf_info in termvector['tf'].items():
                document_length[doc_id] = document_length.get(doc_id, 0) + tf_info['tf']

        self.document_length = document_length

    @timing
    def init_index(self, metadata_file_path=None):
        logging.info("Initializing Index")
        if metadata_file_path:
            self.metadata = self._read_metadata_from_file(metadata_file_path)

        if not self.metadata:
            raise RuntimeError('Metadata cannot be none while initializing the index')

        self.catalog = self._read_catalog_to_file(self.metadata['catalog_file_path'])
        self.index_file_handle = open(self.metadata['index_file_path'], 'rb')
        self._compute_document_length()
        logging.info("Index initialized")

    def index_documents(self, documents, index_head, enable_stemming):
        metadata_list = Utils.run_tasks_parallelly_in_chunks(self._create_documents_index_and_catalog, documents,
                                                             Constants.NO_OF_PARALLEL_INDEXING_TASKS,
                                                             index_head=index_head,
                                                             enable_stemming=enable_stemming)

        merged_metadata = self._merge_indexes_and_catalogs(metadata_list)
        merged_metadata_file_path = self._write_metadata_to_file(merged_metadata)
        self._make_files_readonly(merged_metadata_file_path, merged_metadata)
        self.metadata = merged_metadata
        self.init_index()
        return merged_metadata, merged_metadata_file_path

    @lru_cache(maxsize=Constants.TERMVECTOR_CACHE_SIZE)
    def get_termvector(self, term):
        tf_metadata = self.catalog['data'].get(term)
        if tf_metadata:
            return self._read_termvector(self.index_file_handle, tf_metadata['pos'], tf_metadata['size'])
        else:
            return {}

    def get_total_documents(self):
        return self.catalog['metadata']['total_docs']

    def get_vocabulary_size(self) -> int:
        return len(self.catalog['data'])

    def get_average_doc_length(self) -> float:
        tf_sum = 0
        for term in self.catalog['data'].keys():
            termvector = self.get_termvector(term)
            for doc_id, tf_info in termvector['tf'].items():
                tf_sum += tf_info['tf']

        return tf_sum / self.get_total_documents()

    def analyze(self, text: str, enable_stemming: bool) -> list:
        tokens = self.tokenizer.tokenize(text)
        tokens = self.stopwords_filter.filter(tokens)

        if enable_stemming:
            tokens = [(self.stemmer.stem(token[0]), token[1]) for token in tokens]

        return tokens

    def get_doc_length(self, document_id) -> int:
        return self.document_length.get(document_id)

    def get_all_document_ids(self):
        return self.document_length.keys()

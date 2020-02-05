import datetime
import json
import logging
import os
from functools import lru_cache

from HW_2.compressor import Compressor
from HW_2.serializer import Serializer
from constants.constants import Constants
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

        self._create_dirs_if_absent()

    def _create_dirs_if_absent(self):
        for path in [self._get_metadata_dir(), self._get_index_data_dir(), self._get_catalog_data_dir()]:
            if not os.path.isdir(path):
                os.makedirs(path)

    @classmethod
    def _calculate_and_update_tf_info(cls, document_id, tokens, tf_info):
        for token in tokens:
            term = token[0]
            if term not in tf_info:
                tf_info[term] = {'ttf': 0, 'tf': {}}

            # updating the ttf
            tf_info[term]['ttf'] += 1

            term_tf_info = tf_info[term]['tf']
            if document_id not in term_tf_info:
                term_tf_info[document_id] = {'tf': 0, 'pos': []}

            termvector = term_tf_info[document_id]
            # updating the tf
            termvector['tf'] += 1

            # updating the position information
            termvector['pos'].append(token[1])

    @classmethod
    def _merge_tf_infos(cls, term, tf_info_1, tf_info_2):
        merged_tf_info = {term: {'ttf': 0, 'tf': {}}}

        # Merging the ttf
        merged_tf_info[term]['ttf'] = tf_info_1[term]['ttf'] + tf_info_2[term]['ttf']

        # Merging the tf and positions
        merged_term_tf_info = merged_tf_info[term]['tf']
        for document_id, term_tf_info_1 in tf_info_1[term]['tf'].items():
            if document_id not in merged_term_tf_info:
                merged_term_tf_info[document_id] = {'tf': 0, 'pos': []}

            merged_term_tf_info[document_id]['tf'] += term_tf_info_1['tf']
            merged_term_tf_info[document_id]['pos'].extend(term_tf_info_1['pos'])

            term_tf_info_2 = tf_info_2[term]['tf'].get(document_id)
            if term_tf_info_2:
                merged_term_tf_info[document_id]['tf'] += term_tf_info_2['tf']
                merged_term_tf_info[document_id]['pos'].extend(term_tf_info_2['pos'])

        for document_id, term_tf_info_2 in tf_info_2[term]['tf'].items():
            if document_id not in tf_info_1[term]['tf']:
                merged_term_tf_info[document_id] = {'tf': 0, 'pos': []}
                merged_term_tf_info[document_id]['tf'] += term_tf_info_2['tf']
                merged_term_tf_info[document_id]['pos'].extend(term_tf_info_2['pos'])

        return merged_tf_info

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

    def _write_tf_info_to_index_file(self, tf_info):
        catalog = {}
        index_file_path = self._get_new_index_file_path()

        with open(index_file_path, 'wb') as file:
            for term, info in tf_info.items():
                data = {term: info}

                current_pos = file.tell()
                size = self._write_bytes(file, data)

                file.write(b"\n")

                catalog[term] = {'pos': current_pos, 'size': size}

        return catalog, index_file_path

    def _write_catalog_to_file(self, catalog):
        catalog_file_path = self._get_new_catalog_file_path()
        with open(catalog_file_path, 'wb') as file:
            self._write_bytes(file, catalog)

        return catalog_file_path

    def _read_catalog_to_file(self, catalog_file_path):
        with open(catalog_file_path, 'rb') as file:
            catalog = self._read_bytes(file)

        return catalog

    @classmethod
    def _read_metadata_from_file(cls, metadata_file_path):
        with open(metadata_file_path, 'r') as file:
            metadata = json.loads(file.read())

        return metadata

    def _write_metadata_to_file(self, metadata):
        metadata_file_path = self._get_new_metadata_file_path()
        logging.info('Metadata path:{}'.format(metadata_file_path))
        with open(metadata_file_path, 'w') as file:
            file.write(json.dumps(metadata, indent=True))
        return metadata_file_path

    def _create_metadata(self, catalog_file_path, index_file_path):
        return {
            'index_file_path': index_file_path,
            'catalog_file_path': catalog_file_path,
            'compressor': self.compressor.name,
            'serializer': self.serializer.name,
            'timestamp': str(datetime.datetime.now())
        }

    def _create_documents_index_and_catalog(self, documents, index_head, enable_stemming):

        tf_info = {}
        for document in documents:
            tokens = self.tokenizer.tokenize(document['text'])

            if index_head:
                head_tokens = self.tokenizer.tokenize(document['head'])
                tokens.extend(head_tokens)

            tokens = self.stopwords_filter.filter(tokens)

            if enable_stemming:
                tokens = [(self.stemmer.stem(token[0]), token[1]) for token in tokens]

            self._calculate_and_update_tf_info(document['id'], tokens, tf_info)

        catalog, index_file_path = self._write_tf_info_to_index_file(tf_info)
        catalog_file_path = self._write_catalog_to_file(catalog)
        metadata = self._create_metadata(catalog_file_path, index_file_path)
        return metadata

    def _read_bytes(self, file, start=0, size=-1):
        file.seek(start)
        compressed_bytes = file.read(size)
        serialized_string = self.compressor.decompress_bytes_to_string(compressed_bytes)
        return self.serializer.deserialize(serialized_string)

    def _write_bytes(self, file, data):
        serialized_string = self.serializer.serialize(data)
        compressed_bytes = self.compressor.compress_string_to_bytes(serialized_string)
        return file.write(compressed_bytes)

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
        merged_catalog = {}
        with open(merged_index_path, 'wb') as merged_index_file, \
                open(metadata_1['index_file_path'], 'rb') as index_file_1, \
                open(metadata_2['index_file_path'], 'rb') as index_file_2:

            for term, read_metadata_1 in catalog_1.items():
                tf_info_1 = self._read_bytes(index_file_1, read_metadata_1['pos'], read_metadata_1['size'])
                if term in catalog_2:
                    read_metadata_2 = catalog_2[term]
                    tf_info_2 = self._read_bytes(index_file_2, read_metadata_2['pos'], read_metadata_2['size'])
                    merged_tf_info = self._merge_tf_infos(term, tf_info_1, tf_info_2)
                else:
                    merged_tf_info = tf_info_1

                pos = merged_index_file.tell()
                size = self._write_bytes(merged_index_file, merged_tf_info)
                merged_index_file.write(b'\n')
                merged_catalog[term] = {'pos': pos, 'size': size}

            for term, read_metadata_2 in catalog_2.items():
                if term not in catalog_1:
                    tf_info_2 = self._read_bytes(index_file_2, read_metadata_2['pos'], read_metadata_2['size'])

                    pos = merged_index_file.tell()
                    size = self._write_bytes(merged_index_file, tf_info_2)
                    merged_index_file.write(b'\n')
                    merged_catalog[term] = {'pos': pos, 'size': size}

        self._delete_index_and_catalog_files(metadata_1)
        self._delete_index_and_catalog_files(metadata_2)

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

    def init_index(self, metadata_file_path=None):
        if metadata_file_path:
            self.metadata = self._read_metadata_from_file(metadata_file_path)

        if not self.metadata:
            raise RuntimeError('Metadata cannot be none while initializing the index')

        self.catalog = self._read_catalog_to_file(self.metadata['catalog_file_path'])
        self.index_file_handle = open(self.metadata['index_file_path'], 'rb')

    def index_documents(self, documents, index_head, enable_stemming):
        metadata_list = Utils.run_tasks_parallelly_in_chunks(self._create_documents_index_and_catalog, documents,
                                                             Constants.NO_OF_PARALLEL_INDEXING_TASKS,
                                                             index_head=index_head,
                                                             enable_stemming=enable_stemming)

        merged_metadata = self._merge_indexes_and_catalogs(metadata_list)
        metadata_file_path = self._write_metadata_to_file(merged_metadata)
        self._make_files_readonly(metadata_file_path, merged_metadata)
        self.metadata = merged_metadata
        self.init_index()
        return merged_metadata

    @lru_cache(maxsize=Constants.TERMVECTOR_CACHE_SIZE)
    def get_termvector(self, term):
        logging.info("inside get_termvector:::term: {}".format(term))
        tf_metadata = self.catalog.get(term)
        if tf_metadata:
            return self._read_bytes(self.index_file_handle, tf_metadata['pos'], tf_metadata['size'])
        else:
            return {}

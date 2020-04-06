import json
import logging
from collections import defaultdict
from typing import Dict, List

import numpy as np
from sklearn.model_selection import train_test_split

from constants.constants import Constants
from utils.decorators import timing
from utils.utils import Utils


class FeatureGenerator:
    _IR_FEATURES = ['okapi_bm25', 'okapi_tf', 'okapi_tf_idf', 'unigram_lm_with_jelinek_mercer_smoothing',
                    'unigram_lm_with_laplace_smoothing']

    _IR_FEATURE_INDEXES = {feature: ix for ix, feature in enumerate(_IR_FEATURES)}
    _TREQ_FILE_PATHS = {ir_func: '{}/{}_all.txt'.format(Constants.HW_1_RESULT_DIR, ir_func) for ir_func in _IR_FEATURES}
    _CACHE_PATH = {
        'features': 'feature_matrix_cache/features.json',
        'labels': 'feature_matrix_cache/labels.json'
    }

    @timing
    def _generate_IR_features(self, query_document_mapping) -> Dict[str, Dict[str, List[float]]]:
        logging.info("Generating IR features")
        feature_dict = defaultdict(lambda: defaultdict(lambda: [0] * len(self._IR_FEATURES)))
        for ir_feature in self._IR_FEATURES:
            treq = Utils.parse_treq_file(self._TREQ_FILE_PATHS[ir_feature])
            feature_index = self._IR_FEATURE_INDEXES[ir_feature]
            for doc_type in ['relevant', 'non_relevant']:
                for query_id in query_document_mapping.keys():
                    for doc_id in query_document_mapping[query_id][doc_type]:
                        feature_dict[query_id][doc_id][feature_index] = treq[query_id][doc_id]

        logging.info("IR features generated")
        return feature_dict

    @classmethod
    def _generate_labels(cls, query_document_mapping: Dict[str, Dict[str, List[str]]]) -> Dict[str, Dict[str, int]]:
        logging.info("Generating labels")
        labels_dict = defaultdict(dict)

        for query_id in query_document_mapping.keys():
            for doc_id in query_document_mapping[query_id]['relevant']:
                labels_dict[query_id][doc_id] = 1

            for doc_id in query_document_mapping[query_id]['non_relevant']:
                labels_dict[query_id][doc_id] = 0

        logging.info("Labels generated")
        return labels_dict

    @classmethod
    @timing
    def _transform_dict_to_np_array(cls, query_ids, feature_dict, label_dict):
        feature_matrix = []
        labels = []
        index = []

        for query_id in query_ids:
            for doc_id, features in feature_dict[query_id].items():
                feature_matrix.append(features)
                labels.append(label_dict[query_id][doc_id])

                index.append((query_id, doc_id))

        X = np.array(feature_matrix)
        Y = np.array(labels)
        index = np.array(index)

        np.testing.assert_(X.T.shape[0] == len(cls._IR_FEATURES), "Feature matrix shape mismatch")
        np.testing.assert_(X.shape[0] == Y.shape[0], "Feature label size mismatch")
        return X, Y, index

    @classmethod
    def _add_non_relevant_documents(cls, query_id, query_document_mapping, treq_query_doc_id_mappings):
        if len(query_document_mapping[query_id]['non_relevant']) < 1000:
            tups = treq_query_doc_id_mappings[query_id].items()

            for tup in sorted(tups, key=lambda x: x[1], reverse=False):
                if len(query_document_mapping[query_id]['non_relevant']) == 1000:
                    break

                doc_id = tup[0]
                if doc_id not in query_document_mapping[query_id]['relevant']:
                    query_document_mapping[query_id]['non_relevant'].append(doc_id)

    @classmethod
    def _get_qrel_doc_ids(cls) -> Dict[str, Dict[str, int]]:
        logging.info('Parsing QREL for doc ids')
        qrel_file_path = Utils.get_qrel_file_path()
        query_doc_id_mapping = defaultdict(dict)
        with open(qrel_file_path, 'r') as file:
            for line in file:
                parts = line.strip().split(" ")
                query_id = parts[0].strip()
                doc_id = parts[2].strip()
                relevance = int(parts[3].strip())

                query_doc_id_mapping[query_id][doc_id] = relevance

        logging.info("QREL file parsed")
        return query_doc_id_mapping

    @classmethod
    def _get_document_set_for_queries(cls, queries, bm25_treq_file_path) -> Dict:
        # dir_path = Utils.get_ap89_collection_abs_path()
        # file_paths = get_file_paths_to_parse(dir_path)
        # all_documents = {doc['id']: doc for doc in get_parsed_documents(file_paths)}

        qrel_query_doc_id_mappings = cls._get_qrel_doc_ids()
        treq_query_doc_id_mappings = Utils.parse_treq_file(bm25_treq_file_path)

        query_document_mapping = defaultdict(lambda: defaultdict(list))
        for query in queries:
            query_id = query['id']
            for doc_id, relevance in qrel_query_doc_id_mappings[query_id].items():
                if relevance > 0:
                    query_document_mapping[query_id]['relevant'].append(doc_id)
                else:
                    query_document_mapping[query_id]['non_relevant'].append(doc_id)

            cls._add_non_relevant_documents(query_id, query_document_mapping, treq_query_doc_id_mappings)

            assert len(set(query_document_mapping[query_id]['relevant']).intersection(
                set(query_document_mapping[query_id]['non_relevant']))) == 0, \
                "Found common documents in relevant and non relevant list"

        return query_document_mapping

    @timing
    def generate_features(self, queries, use_cached=True):
        if use_cached:
            with open(self._CACHE_PATH['features'], 'r') as features_file, \
                    open(self._CACHE_PATH['labels'], 'r') as labels_file:
                feature_dict = json.load(features_file)
                label_dict = json.load(labels_file)
        else:
            bm25_file_path = '{}/HW_1/results/okapi_bm25_all.txt'.format(Constants.PROJECT_ROOT)
            query_document_mapping = self._get_document_set_for_queries(queries, bm25_file_path)

            feature_dict = self._generate_IR_features(query_document_mapping)
            label_dict = self._generate_labels(query_document_mapping)

            with open(self._CACHE_PATH['features'], 'w') as features_file, \
                    open(self._CACHE_PATH['labels'], 'w') as labels_file:
                json.dump(feature_dict, features_file)
                json.dump(label_dict, labels_file)

        query_ids = [query['id'] for query in queries]
        train_query_ids, test_query_ids = train_test_split(query_ids, test_size=0.2)

        X_train, Y_train, train_index = self._transform_dict_to_np_array(train_query_ids, feature_dict, label_dict)
        X_test, Y_test, test_index = self._transform_dict_to_np_array(test_query_ids, feature_dict, label_dict)
        return X_train, X_test, train_index, Y_train, Y_test, test_index

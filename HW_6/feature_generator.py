import logging
from collections import defaultdict
from typing import Dict, List

import numpy as np

from constants.constants import Constants
from utils.utils import Utils


class FeatureGenerator:
    _IR_FEATURES = ['okapi_bm25', 'okapi_tf', 'okapi_tf_idf', 'unigram_lm_with_jelinek_mercer_smoothing',
                    'unigram_lm_with_laplace_smoothing']

    _IR_FEATURE_INDEXES = {feature: ix for ix, feature in enumerate(_IR_FEATURES)}
    _TREQ_FILE_PATHS = {ir_func: '{}/{}_all.txt'.format(Constants.HW_1_RESULT_DIR, ir_func) for ir_func in _IR_FEATURES}

    @classmethod
    def _get_key(cls, query_id, doc_id):
        return '{}-{}'.format(query_id, doc_id)

    def _generate_IR_features(self, query_document_mapping) -> Dict[str, Dict[str, List[float]]]:
        logging.info("Generating IR features")
        feature_dict = defaultdict(lambda: defaultdict(list))
        for ir_feature in self._IR_FEATURES:
            treq = Utils.parse_treq_file(self._TREQ_FILE_PATHS[ir_feature])

            for doc_type in ['relevant', 'non_relevant']:
                for query_id in query_document_mapping.keys():
                    for doc_id in query_document_mapping[query_id][doc_type]:
                        feature_dict[query_id][doc_id].append(treq[query_id][doc_id])

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
    def _transform_dict_to_np_array(cls, query_ids, feature_dict, label_dict):
        feature_matrix = []
        labels = []
        for query_id in query_ids:
            for doc_id, features in feature_dict[query_id].items():
                feature_matrix.append(features)
                labels.append(label_dict[query_id][doc_id])

        X = np.array(feature_matrix)
        Y = np.array(labels)

        np.testing.assert_(X.shape[0] == Y.shape[0], "Feature label size mismatch")
        return X, Y

    def generate_features(self, train_query_ids, test_query_ids,
                          query_document_mapping: Dict[str, Dict[str, List[str]]]):

        feature_dict = self._generate_IR_features(query_document_mapping)
        label_dict = self._generate_labels(query_document_mapping)

        X_train, Y_train = self._transform_dict_to_np_array(train_query_ids, feature_dict, label_dict)
        X_test, Y_test = self._transform_dict_to_np_array(test_query_ids, feature_dict, label_dict)
        return X_train, X_test, Y_train, Y_test

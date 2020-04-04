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

    def _generate_IR_features(self, query_document_mapping: Dict[str, Dict[str, List[str]]]) -> Dict[str, List[float]]:
        logging.info("Generating IR features")
        feature_dict = defaultdict(list)
        for ir_feature in self._IR_FEATURES:
            treq = Utils.parse_treq_file(self._TREQ_FILE_PATHS[ir_feature])

            for doc_type in ['relevant', 'non_relevant']:
                for query_id in query_document_mapping.keys():
                    for doc_id in query_document_mapping[query_id][doc_type]:
                        feature_dict[self._get_key(query_id, doc_id)].append(treq[query_id][doc_id])

        logging.info("IR features generated for {} rows".format(len(feature_dict)))
        return feature_dict

    def _generate_labels(self, query_document_mapping: Dict[str, Dict[str, List[str]]]) -> Dict[str, int]:
        logging.info("Generating labels")
        labels_dict = {}

        for query_id in query_document_mapping.keys():
            for doc_id in query_document_mapping[query_id]['relevant']:
                labels_dict[self._get_key(query_id, doc_id)] = 1

            for doc_id in query_document_mapping[query_id]['non_relevant']:
                labels_dict[self._get_key(query_id, doc_id)] = 0

        logging.info("Labels generated for {} rows".format(len(labels_dict)))
        return labels_dict

    def generate_features(self, query_document_mapping: Dict[str, Dict[str, List[str]]]):
        feature_dict = self._generate_IR_features(query_document_mapping)
        labels_dict = self._generate_labels(query_document_mapping)
        np.testing.assert_array_equal(feature_dict.keys(), labels_dict.keys(), "Feature, label keys mismatch")

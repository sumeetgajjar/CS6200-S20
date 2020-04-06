import logging
import random
from collections import defaultdict
from typing import Dict

import numpy as np
from sklearn.linear_model import LinearRegression

from HW_1.main import parse_queries, transform_scores_for_writing_to_file
from HW_5.treq_eval import TREQEval
from HW_6.feature_generator import FeatureGenerator
from constants.constants import Constants
from utils.decorators import timing
from utils.utils import Utils


class HW6:
    _TREQ_FILE_SPLIT_REGEX = '\\s+'

    @classmethod
    def _get_qrel_doc_ids(cls) -> Dict[str, Dict[str, int]]:
        logging.info('Parsing QREL for doc ids')
        qrel_file_path = cls._get_qrel_file_path()
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
    def _get_qrel_file_path(cls):
        return '{}/{}'.format(Utils.get_ap_data_path(), 'qrels.adhoc.51-100.AP89.txt')

    @classmethod
    def _add_non_relevant_documents(cls, query_id, query_document_mapping, treq_query_doc_id_mappings):
        if len(query_document_mapping[query_id]['non_relevant']) < 1000:
            tups = treq_query_doc_id_mappings[query_id].items()

            for tup in sorted(tups, key=lambda x: x[1], reverse=True):
                if len(query_document_mapping[query_id]['non_relevant']) == 1000:
                    break

                doc_id = tup[0]
                if doc_id not in query_document_mapping[query_id]['relevant']:
                    query_document_mapping[query_id]['non_relevant'].append(doc_id)

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

    @classmethod
    @timing
    def _run_model(cls, queries, model, model_name, fold_num, X_train, X_test, train_index, Y_train, Y_test,
                   test_index):
        logging.info("Running {} fold:{}".format(model_name, fold_num))
        model.fit(X_train, Y_train)

        def _run_prediction_phase(phase_name, X, index):
            logging.info("Running {} predictions".format(phase_name))
            Y_predict = model.predict(X)

            rankings = defaultdict(list)
            for ix, prediction in enumerate(Y_predict):
                query_id, doc_id = index[ix]
                rankings[query_id].append((prediction, doc_id))

            results_to_write = []
            for query in queries:
                scores = rankings[query['id']]
                scores.sort(reverse=True)
                results_to_write.extend(transform_scores_for_writing_to_file(scores, query))

            treq_file = 'results/{}-performance/{}-{}.txt'.format(phase_name, model_name, fold_num)
            Utils.write_results_to_file(treq_file,
                                        results_to_write)

            map = TREQEval(cls._get_qrel_file_path(), treq_file, False).eval()
            return map

        training_map = _run_prediction_phase('training', X_train, train_index)
        testing_map = _run_prediction_phase('testing', X_test, test_index)
        return training_map, testing_map

    @classmethod
    def main(cls):
        queries = parse_queries()
        query_ids = [query['id'] for query in queries]

        bm25_file_path = '{}/HW_1/results/okapi_bm25_all.txt'.format(Constants.PROJECT_ROOT)
        query_document_mapping = cls._get_document_set_for_queries(queries, bm25_file_path)

        mean_MAP = {}
        for model, model_name in [
            (LinearRegression(), 'linear-regression'),
            # (LogisticRegression(), 'logistic-regression'),
            # (DecisionTreeRegressor(max_depth=2), 'decision-tree-2'),
            # (DecisionTreeRegressor(max_depth=5), 'decision-tree-5'),
            # (DecisionTreeRegressor(max_depth=7), 'decision-tree-7'),
            # (DecisionTreeRegressor(max_depth=10), 'decision-tree-10'),
            # (DecisionTreeRegressor(max_depth=15), 'decision-tree-15'),
            # (DecisionTreeRegressor(max_depth=15), 'decision-tree-20'),
            # (GradientBoostingRegressor(n_estimators=400, max_depth=3, min_samples_split=2, learning_rate=0.01,
            #                            verbose=1), 'boosting-trees')
        ]:
            k_folds = 5
            sum_training_map, sum_testing_map = 0, 0

            for i in range(k_folds):
                X_train, X_test, train_index, Y_train, Y_test, test_index = FeatureGenerator().generate_features(
                    query_ids, query_document_mapping, use_cached=True)
                training_map, testing_map = cls._run_model(queries, model, model_name, i + 1, X_train, X_test,
                                                           train_index, Y_train, Y_test, test_index)
                sum_training_map += training_map
                sum_testing_map += testing_map

            mean_MAP[model_name] = (sum_training_map / k_folds, sum_testing_map / k_folds)

        for model_name, MAP in mean_MAP.items():
            logging.info("{} Training Mean MAP:{}, Testing Mean MAP:{}".format(model_name, MAP[0], MAP[1]))


if __name__ == '__main__':
    np.random.seed(134)
    random.seed(134)

    Utils.configure_logging()
    HW6.main()

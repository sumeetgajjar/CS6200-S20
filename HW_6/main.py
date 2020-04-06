import logging
import random
from collections import defaultdict

import numpy as np
from sklearn.linear_model import LinearRegression

from HW_1.main import parse_queries, transform_scores_for_writing_to_file
from HW_5.treq_eval import TREQEval
from HW_6.feature_generator import FeatureGenerator
from utils.decorators import timing
from utils.utils import Utils


class HW6:
    _TREQ_FILE_SPLIT_REGEX = '\\s+'

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

            map = TREQEval(Utils.get_qrel_file_path(), treq_file, False).eval()
            return map

        training_map = _run_prediction_phase('training', X_train, train_index)
        testing_map = _run_prediction_phase('testing', X_test, test_index)
        return training_map, testing_map

    @classmethod
    def main(cls):
        queries = parse_queries()

        mean_MAP = {}
        for model, model_name in [
            (LinearRegression(normalize=True), 'linear-regression'),
            # (LogisticRegression(), 'logistic-regression'),
            # (ElasticNet(), 'elastic-net'),
            # (DecisionTreeRegressor(max_depth=2), 'decision-tree-2'),
            # (DecisionTreeRegressor(max_depth=5), 'decision-tree-5'),
            # (DecisionTreeRegressor(max_depth=7), 'decision-tree-7'),
            # (DecisionTreeRegressor(max_depth=10), 'decision-tree-10'),
            # (DecisionTreeRegressor(max_depth=15), 'decision-tree-15'),
            # (DecisionTreeRegressor(max_depth=20), 'decision-tree-20'),
            # (GradientBoostingRegressor(n_estimators=400, max_depth=3, min_samples_split=2, learning_rate=0.01,
            #                            verbose=1), 'boosting-trees')
        ]:
            k_folds = 5
            sum_training_map, sum_testing_map = 0, 0

            for i in range(k_folds):
                X_train, X_test, train_index, Y_train, Y_test, test_index = FeatureGenerator().generate_features(
                    queries, use_cached=True)
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

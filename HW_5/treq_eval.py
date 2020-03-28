import logging
import re
import sys
from collections import defaultdict
from typing import Dict

from utils.utils import Utils


class TREQEval:

    def __init__(self, qrel_file_path: str, treq_file_path: str, print_all_queries: bool,
                 enable_strong_relevance: bool = False) -> None:
        self.qrel_file_path = qrel_file_path
        self.treq_file_path = treq_file_path
        self.print_all_queries = print_all_queries
        self.split_regex = re.compile("\\s+")
        self.enable_strong_relevance = enable_strong_relevance

    def _parse_qrel_file(self) -> Dict[str, Dict[str, bool]]:
        logging.info("Parsing Qrel file: {}".format(self.qrel_file_path))
        qrel = defaultdict(dict)
        with open(self.qrel_file_path, 'r', encoding='utf-8-sig') as file:
            for line in file:
                query_id, assessor, doc_id, relevance = re.split(self.split_regex, line.strip())
                qrel[query_id][doc_id] = 1 if int(relevance) >= 1 else 0

        logging.info("Qrel file parsed")
        return qrel

    def _parse_treq_file(self) -> Dict[str, Dict[str, float]]:
        logging.info("Parsing Treq file: {}".format(self.treq_file_path))
        treq = defaultdict(dict)
        with open(self.treq_file_path, 'r', encoding='utf-8-sig') as file:
            for line in file:
                query_id, _, doc_id, _, score, _ = re.split(self.split_regex, line.strip())
                treq[query_id][doc_id] = float(score)

        logging.info("Treq file parsed")
        return treq

    def eval(self):
        qrel = self._parse_qrel_file()
        logging.info(qrel)
        treq = self._parse_treq_file()
        logging.info(treq)


if __name__ == '__main__':
    Utils.configure_logging()
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        logging.error("Usage: treq_eval.py [-q] qrel_file_path treq_file_path")
        sys.exit(1)

    print_all_queries = len(sys.argv) == 3
    treq_eval = TREQEval(sys.argv[-2], sys.argv[-1], print_all_queries)
    treq_eval.eval()

import logging
import re
import sys
from collections import defaultdict
from typing import Dict, Tuple

from utils.utils import Utils


class TREQEval:
    _RECALLS = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
    _CUTOFFS = (5, 10, 15, 20, 30, 100, 200, 500, 1000)
    _MAX_DOCS_TO_CONSIDER = 1000

    def __init__(self, qrel_file_path: str, treq_file_path: str, print_all_queries: bool,
                 enable_strong_relevance: bool = False) -> None:
        self.qrel_file_path = qrel_file_path
        self.treq_file_path = treq_file_path
        self.print_all_queries = print_all_queries
        self.split_regex = re.compile("\\s+")
        self.enable_strong_relevance = enable_strong_relevance

    def _parse_qrel_file(self) -> Tuple[Dict[str, Dict[str, bool]], Dict[str, int]]:
        logging.info("Parsing Qrel file: {}".format(self.qrel_file_path))
        qrel = defaultdict(dict)
        num_relevance = defaultdict(int)

        with open(self.qrel_file_path, 'r', encoding='utf-8-sig') as file:
            for line in file:
                query_id, assessor, doc_id, relevance = re.split(self.split_regex, line.strip())
                qrel[query_id][doc_id] = 1 if int(relevance) >= 1 else 0
                num_relevance += qrel[query_id][doc_id]

        logging.info("Qrel file parsed")
        return qrel, num_relevance

    def _parse_treq_file(self) -> Dict[str, Dict[str, float]]:
        logging.info("Parsing Treq file: {}".format(self.treq_file_path))
        treq = defaultdict(dict)
        with open(self.treq_file_path, 'r', encoding='utf-8-sig') as file:
            for line in file:
                query_id, _, doc_id, _, score, _ = re.split(self.split_regex, line.strip())
                treq[query_id][doc_id] = float(score)

        logging.info("Treq file parsed")
        return treq

    @classmethod
    def _get_sorted_doc_ids(cls, doc_info_dict: Dict[str, float]):
        asc_sorted_doc_ids = sorted(doc_info_dict.items(), key=lambda tup: tup[0])
        desc_sorted_scores = sorted(asc_sorted_doc_ids, key=lambda tup: tup[1], reverse=True)
        return map(lambda tup: tup[0], desc_sorted_scores)

    def _print_stats(self, query_id, ret, rel, rel_ret, prec_at_recalls, mean_avg_precision, prec_at_cutoffs, r_prec):
        result_str = ''
        result_str += "\nQueryid (Num):    {}\n".format(query_id)
        result_str += "Total number of documents over all queries\n"
        result_str += "    Retrieved:    {}\n".format(ret)
        result_str += "    Relevant:     {}\n".format(rel)
        result_str += "    Rel_ret:      {}\n".format(rel_ret)
        result_str += "Interpolated Recall - Precision Averages:\n"
        result_str += "    at 0.00       %.4f\n".format(prec_at_recalls[0])
        result_str += "    at 0.10       %.4f\n".format(prec_at_recalls[1])
        result_str += "    at 0.20       %.4f\n".format(prec_at_recalls[2])
        result_str += "    at 0.30       %.4f\n".format(prec_at_recalls[3])
        result_str += "    at 0.40       %.4f\n".format(prec_at_recalls[4])
        result_str += "    at 0.50       %.4f\n".format(prec_at_recalls[5])
        result_str += "    at 0.60       %.4f\n".format(prec_at_recalls[6])
        result_str += "    at 0.70       %.4f\n".format(prec_at_recalls[7])
        result_str += "    at 0.80       %.4f\n".format(prec_at_recalls[8])
        result_str += "    at 0.90       %.4f\n".format(prec_at_recalls[9])
        result_str += "    at 1.00       %.4f\n".format(prec_at_recalls[10])
        result_str += "Average precision (non-interpolated) for all rel docs(averaged over queries)\n"
        result_str += "                  %.4f\n".format(mean_avg_precision)
        result_str += "Precision:\n"
        result_str += "  At    5 docs:   %.4f\n".format(prec_at_cutoffs[0])
        result_str += "  At   10 docs:   %.4f\n".format(prec_at_cutoffs[1])
        result_str += "  At   15 docs:   %.4f\n".format(prec_at_cutoffs[2])
        result_str += "  At   20 docs:   %.4f\n".format(prec_at_cutoffs[3])
        result_str += "  At   30 docs:   %.4f\n".format(prec_at_cutoffs[4])
        result_str += "  At  100 docs:   %.4f\n".format(prec_at_cutoffs[5])
        result_str += "  At  200 docs:   %.4f\n".format(prec_at_cutoffs[6])
        result_str += "  At  500 docs:   %.4f\n".format(prec_at_cutoffs[7])
        result_str += "  At 1000 docs:   %.4f\n".format(prec_at_cutoffs[8])
        result_str += "R-Precision (precision after R (= num_rel for a query) docs retrieved):\n"
        result_str += "    Exact:        %.4f\n".format(r_prec)

    def eval(self):
        qrel, num_relevance = self._parse_qrel_file()
        treq = self._parse_treq_file()

        tot_num_ret = 0
        tot_num_rel = 0
        tot_num_rel_ret = 0
        sum_prec_at_cutoffs = []
        sum_prec_at_recalls = []
        avg_prec_at_cutoffs = []
        avg_prec_at_recalls = []
        sum_avg_prec = 0
        sum_r_prec = 0

        num_topics = 0
        for query_id in sorted(treq.keys()):
            if not num_relevance[query_id]:
                continue

            num_topics += 1
            prec_list = []
            rec_list = []

            num_ret = 0
            num_rel_ret = 0
            sum_prec = 0

            for doc_id in self._get_sorted_doc_ids(treq[query_id]):
                num_ret += 1
                rel = qrel[query_id].get(doc_id)
                if rel:
                    sum_prec += rel * (1 + num_rel_ret) / num_ret
                    num_rel_ret += rel

                prec_list.append(num_rel_ret / num_ret)
                rec_list.append(num_rel_ret / num_relevance[query_id])

                if num_ret >= self._MAX_DOCS_TO_CONSIDER:
                    break

            avg_precision = sum_prec / num_relevance[query_id]
            final_recall = num_rel_ret / num_relevance[query_id]

            for ix in range(num_ret, self._MAX_DOCS_TO_CONSIDER):
                prec_list.append(num_rel_ret / ix)
                rec_list.append(final_recall)

            prec_at_cutoffs = []
            for cutoff in self._CUTOFFS:
                prec_at_cutoffs.append(prec_list[cutoff - 1])

            if num_relevance[query_id] > num_ret:
                r_prec = num_rel_ret / num_relevance[query_id]
            else:
                int_num_rel = int(num_relevance[query_id])
                frac_num_rel = num_relevance[query_id] - int_num_rel

                r_prec = (1 - frac_num_rel) * prec_list[int_num_rel] + frac_num_rel * prec_list[
                    int_num_rel + 1] if frac_num_rel > 0 else prec_list[int_num_rel]

            max_prec = 0
            for ix in range(self._MAX_DOCS_TO_CONSIDER - 1, -1, -1):
                if prec_list[ix] > max_prec:
                    max_prec = prec_list[ix]
                else:
                    prec_list[ix] = max_prec

            prec_at_recalls = []
            ix = 0
            for recall in self._RECALLS:
                while ix < self._MAX_DOCS_TO_CONSIDER and rec_list[ix] < recall:
                    ix += 1

                if ix < self._MAX_DOCS_TO_CONSIDER:
                    prec_at_recalls.append(prec_list[ix])
                else:
                    prec_at_recalls.append(0)

            if self.print_all_queries:
                self._print_stats(query_id, num_ret, num_relevance[query_id],
                                  num_rel_ret, prec_at_recalls, avg_precision,
                                  prec_at_cutoffs, r_prec)

            tot_num_ret += num_ret
            tot_num_rel += num_relevance[query_id]
            tot_num_rel_ret += num_rel_ret

            for ix in range(len(self._CUTOFFS)):
                sum_prec_at_cutoffs[ix] += prec_at_cutoffs[ix]

            for ix in range(len(self._RECALLS)):
                sum_prec_at_recalls[ix] += prec_at_recalls[ix]

            sum_avg_prec += avg_precision
            sum_r_prec += r_prec

        for ix in range(len(self._CUTOFFS)):
            avg_prec_at_cutoffs[ix] = sum_prec_at_cutoffs[ix] / num_topics

        for ix in range(len(self._RECALLS)):
            avg_prec_at_cutoffs[ix] = sum_prec_at_recalls[ix] / num_topics

        mean_avg_prec = sum_avg_prec / num_topics
        avg_r_prec = sum_r_prec / num_topics

        self._print_stats(num_topics, tot_num_ret, tot_num_rel, tot_num_rel_ret,
                          avg_prec_at_recalls, mean_avg_prec, avg_prec_at_cutoffs, avg_r_prec)


if __name__ == '__main__':
    Utils.configure_logging()
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        logging.error("Usage: treq_eval.py [-q] qrel_file_path treq_file_path")
        sys.exit(1)

    print_all_queries = len(sys.argv) == 3
    treq_eval = TREQEval(sys.argv[-2], sys.argv[-1], print_all_queries)
    treq_eval.eval()

from HW_1.es_utils import EsUtils
from HW_5.treq_eval import TREQEval
from constants.constants import Constants
from utils.utils import Utils


class HW5:
    _QUERIES = [{'id': 152101, 'query': 'founding fathers'},
                {'id': 152102, 'query': 'independence war causes'},
                {'id': 152103, 'query': 'declaration of independence'}]

    @classmethod
    def _query_vertical_search(cls, query):
        es_client = EsUtils.get_es_client()
        response = es_client.search(index=Constants.CRAWLED_DATA_INDEX_NAME, body={
            "query": {
                "query_string": {
                    "query": query['query']
                }
            },
            "size": 1000,
            "stored_fields": []
        })
        results = []
        for ix, doc in enumerate(response['hits']['hits']):
            results.append({
                'doc_no': doc['_id'],
                'rank': ix + 1,
                'score': doc['_score'],
                'query_number': query['id']
            })
        return results

    @classmethod
    def main(cls):
        results_to_write = []
        for query in cls._QUERIES:
            results = cls._query_vertical_search(query)
            results.sort(reverse=True, key=lambda d: d['score'])
            results_to_write.extend(results)

        file_path = 'results/hw_4_queries_result.txt'
        Utils.write_results_to_file(file_path, results_to_write)

        TREQEval('results/qrel.txt', 'results/hw_4_queries_result.txt', True).eval(plot_precision_recall=True,
                                                                                   plot_save_dir='results')


if __name__ == '__main__':
    Utils.configure_logging()
    HW5.main()

import csv
import logging
import random
from typing import Dict, List, Set

from HW_1.es_utils import EsUtils
from HW_4.hits import HITS
from HW_4.page_rank import PageRank
from constants.constants import Constants
from utils.decorators import timing
from utils.utils import Utils, LinkGraph


class PageRankInfo:

    def __init__(self, url: str, pagerank: float, inlinks_count: int, outlinks_count: int) -> None:
        self.url = url
        self.pagerank = pagerank
        self.inlinks_count = inlinks_count
        self.outlinks_count = outlinks_count


class HW4:

    @classmethod
    @timing
    def _get_top_500_links(cls, pageranks: Dict[str, float], linkgraph: LinkGraph):
        logging.info("Getting top 500 urls")
        temp = [PageRankInfo(url, pagerank, len(linkgraph.get_inlinks(url)), len(linkgraph.get_outlinks(url)))
                for url, pagerank in sorted(pageranks.items(), key=lambda tup: tup[1], reverse=True)]
        return temp[:500]

    @classmethod
    @timing
    def _export_pagerank_infos(cls, pagerankinfos: List[PageRankInfo], filepath: str):
        logging.info("Exporting pagerank infos to {}".format(filepath))
        with open(filepath, 'w') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['URL', 'PAGE_RANK', 'INLINKS_COUNT', 'OUTLINKS_COUNT'])
            for pr_info in pagerankinfos:
                csv_writer.writerow([pr_info.url, pr_info.pagerank, pr_info.inlinks_count, pr_info.outlinks_count])
        logging.info("Pagerank infos exported")

    @classmethod
    @timing
    def run_page_rank_on_crawled_data(cls):
        logging.info("Calculating PageRank for Crawled Data")
        link_graph = LinkGraph(Utils.get_crawled_link_graph_csv_path())
        pageranks = PageRank().calculate_pagerank_iteratively(link_graph)
        top_500_urls = cls._get_top_500_links(pageranks, link_graph)
        cls._export_pagerank_infos(top_500_urls, Utils.get_crawled_link_graph_pagerank_path())
        logging.info("PageRank for Crawled Data calculated")

    @classmethod
    @timing
    def run_page_rank_on_other_data(cls):
        logging.info("Calculating PageRank for Other Data")
        link_graph = LinkGraph(Utils.get_other_link_graph_csv_path(), inlinks_format=True)
        pageranks = PageRank().calculate_pagerank_iteratively(link_graph)
        top_500_urls = cls._get_top_500_links(pageranks, link_graph)
        cls._export_pagerank_infos(top_500_urls, Utils.get_other_link_graph_pagerank_path())
        logging.info("PageRank for Other Data calculated")

    @classmethod
    def _create_root_set(cls, linkgraph: LinkGraph, d=200, root_set_size=10000) -> Set[str]:
        logging.info("Creating root set")
        es_client = EsUtils.get_es_client()
        response = es_client.search(index=Constants.CRAWLED_DATA_INDEX_NAME, body={
            "query": {
                "query_string": {
                    "query": "1521 AMERICAN INDEPENDENCE WAR"
                }
            },
            "_source": ["url"],
            "size": 1000
        })
        root_urls = [r['_source']['url'] for r in response['hits']['hits']]
        root_set = set(root_urls)

        for url in root_urls:
            root_set.update(linkgraph.get_outlinks(url))

            inlinks = linkgraph.get_inlinks(url)
            if len(inlinks) < d:
                root_set.update(inlinks)
            else:
                root_set.update(random.choices(list(inlinks), k=d))

        logging.info("Root set created, size:{}".format(len(root_set)))
        return root_set

    @classmethod
    def _write_HITS_score_to_file(cls, scores: Dict[str, float], filepath: str):
        logging.info("Writing HITS scores to {}".format(filepath))
        sorted_scores = [(url, score) for url, score in sorted(scores.items(), key=lambda tup: tup[1], reverse=True)]
        top_500 = sorted_scores[:500]

        with open(filepath, 'w') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(["webpageurl", "score"])
            csv_writer.writerows(top_500)

        logging.info("HITS scores written")

    @classmethod
    def run_HITS_on_crawled_data(cls):
        logging.info("Running HITS on crawled data")
        link_graph = LinkGraph(Utils.get_crawled_link_graph_csv_path())
        root_set = cls._create_root_set(link_graph)
        authority_score, hub_score = HITS().calculate_hub_and_authority_score(link_graph, root_set)
        cls._write_HITS_score_to_file(authority_score, Utils.get_crawled_link_graph_HITS_authority_path())
        cls._write_HITS_score_to_file(hub_score, Utils.get_crawled_link_graph_HITS_hub_path())
        logging.info("HITS executed on crawled data")


if __name__ == '__main__':
    Utils.configure_logging()
    HW4.run_page_rank_on_other_data()
    HW4.run_page_rank_on_crawled_data()
    # HW4.run_HITS_on_crawled_data()

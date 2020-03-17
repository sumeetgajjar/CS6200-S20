import csv
import logging
from typing import Dict, List

from HW_4.page_rank import PageRank
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
                for url, pagerank in sorted(pageranks.items(), key=lambda tup: tup[1])]
        return temp[-500:]

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
        link_graph = LinkGraph(Utils.get_other_link_graph_csv_path())
        pageranks = PageRank().calculate_pagerank_iteratively(link_graph)
        top_500_urls = cls._get_top_500_links(pageranks, link_graph)
        cls._export_pagerank_infos(top_500_urls, Utils.get_other_link_graph_pagerank_path())
        logging.info("PageRank for Other Data calculated")


if __name__ == '__main__':
    Utils.configure_logging()
    # HW4.run_page_rank_on_other_data()
    HW4.run_page_rank_on_crawled_data()

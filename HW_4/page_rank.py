from collections import defaultdict
from typing import Dict

from utils.utils import LinkGraph


class PageRank:

    @classmethod
    def has_converged(cls) -> bool:
        # TODO implement this
        return True

    @classmethod
    def _get_sink_urls(cls, linkgraph: LinkGraph):
        return [url for url in linkgraph.get_all_links() if len(linkgraph.get_outlinks(url)) == 0]

    def calculate_pagerank_iteratively(self, linkgraph: LinkGraph, d: float = 0.85) -> Dict[str, float]:
        N = len(linkgraph.get_all_links())
        initial_pagerank = 1 / N
        pagerank = defaultdict(lambda _: initial_pagerank)

        sink_urls = self._get_sink_urls(linkgraph)
        while not self.has_converged():
            sink_pr = 0.0
            for sink_url in sink_urls:
                sink_pr += pagerank[sink_url]

            new_pagerank = {}
            for p in linkgraph.get_all_links():
                new_pagerank[p] = (1 - d) / N
                new_pagerank[p] += (d * sink_pr / N)

                for q in linkgraph.get_inlinks(p):
                    L_q = len(linkgraph.get_outlinks(q))
                    # TODO confirm the zero case
                    if L_q:
                        new_pagerank[p] += (d * pagerank[q] / L_q)

            pagerank = new_pagerank

        return pagerank

import logging
import math
from collections import defaultdict
from typing import Dict, List, Tuple

from utils.utils import LinkGraph


class PageRank:

    def __init__(self) -> None:
        self.perplexities: List[float] = []

    @classmethod
    def _calculate_perplexity(cls, linkgraph: LinkGraph, pagerank: Dict[str, float]) -> float:
        entropy = 0.0
        for url in linkgraph.get_all_links():
            url_pagerank = pagerank[url]
            entropy += url_pagerank * math.log2(url_pagerank)

        entropy = -entropy
        return 2 ** entropy

    def has_converged(self, linkgraph: LinkGraph, pagerank: Dict[str, float]) -> Tuple[float, bool]:
        perplexity = self._calculate_perplexity(linkgraph, pagerank)
        self.perplexities.append(perplexity)

        converged = False
        if len(self.perplexities) == 4:
            converged = True
            for i in range(3):
                if int(self.perplexities[i]) != int(self.perplexities[i + 1]):
                    converged = False
                    break

            del self.perplexities[0]

        return perplexity, converged

    @classmethod
    def _get_sink_urls(cls, linkgraph: LinkGraph):
        return [url for url in linkgraph.get_all_links() if len(linkgraph.get_outlinks(url)) == 0]

    def calculate_pagerank_iteratively(self, linkgraph: LinkGraph, d: float = 0.85) -> Dict[str, float]:
        self.perplexities = []
        N = len(linkgraph.get_all_links())
        initial_pagerank = 1 / N
        pagerank = defaultdict(lambda: initial_pagerank)

        sink_urls = self._get_sink_urls(linkgraph)
        i = 1
        while True:
            perplexity, converged = self.has_converged(linkgraph, pagerank)
            logging.info("Iteration:{}, Perplexity:{}, Converged:{}".format(i, perplexity, converged))
            if converged:
                break

            sink_pr = 0.0
            for p in sink_urls:
                sink_pr += pagerank[p]

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
            i += 1

        self.perplexities = []
        return pagerank

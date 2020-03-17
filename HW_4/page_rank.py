from typing import Dict

from utils.utils import LinkGraph


class PageRank:

    def calculate_pagerank_iteratively(self, linkgraph: LinkGraph) -> Dict[str, float]:
        # TODO: implement this
        return {url: 1.0 for url in linkgraph.get_all_links()}

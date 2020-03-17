import math
from collections import defaultdict
from typing import Tuple, Dict, Set

from utils.utils import LinkGraph


class HITS:

    @classmethod
    def _update_authority_score(cls, urls: Set[str], hub_scores: Dict[str, float],
                                linkgraph: LinkGraph) -> Dict[str, float]:
        authority_scores = {}
        for p in urls:
            for q in linkgraph.get_inlinks(p):
                authority_scores[p] += hub_scores[q]

        return authority_scores

    @classmethod
    def _update_hub_score(cls, urls: Set[str], authority_scores: Dict[str, float],
                          linkgraph: LinkGraph) -> Dict[str, float]:
        hub_scores = {}
        for p in urls:
            for q in linkgraph.get_outlinks(p):
                hub_scores[p] += authority_scores[q]

        return hub_scores

    @classmethod
    def _normalize_scores(cls, scores: Dict[str, float]) -> Dict[str, float]:
        denominator = math.sqrt(sum([x * x for x in scores.values()]))
        return {url: score / denominator for url, score in scores.items()}

    @classmethod
    def _has_converged(cls):
        return True

    def calculate_hub_and_authority_score(self, linkgraph: LinkGraph, root_set: Set[str]) -> Tuple[Dict[str, float],
                                                                                                   Dict[str, float]]:
        initial_authority_score = 1
        initial_hub_score = 1
        authority_score = defaultdict(lambda: initial_authority_score)
        hub_score = defaultdict(lambda: initial_hub_score)

        while not self._has_converged():
            new_authority_score = self._update_authority_score(root_set, authority_score, linkgraph)
            new_hub_score = self._update_hub_score(root_set, hub_score, linkgraph)
            authority_score = self._normalize_scores(new_authority_score)
            hub_score = self._normalize_scores(new_hub_score)

        return authority_score, hub_score

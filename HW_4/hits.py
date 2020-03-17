from collections import defaultdict
from typing import Tuple, Dict, Set

from utils.utils import LinkGraph


class HITS:

    @classmethod
    def _update_hub_score(cls, urls: Set[str], authority_score: Dict[str, float], linkgraph: LinkGraph) -> Dict[
        str, float]:
        pass

    @classmethod
    def _update_authority_score(cls, urls: Set[str], authority_score: Dict[str, float], linkgraph: LinkGraph) -> Dict[
        str, float]:
        pass

    @classmethod
    def _normalize_scores(cls, scores: Dict[str, float]) -> Dict[str, float]:
        return scores

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

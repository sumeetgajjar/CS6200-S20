import logging
import math
from collections import defaultdict
from typing import Tuple, Dict, Set, List

from utils.utils import LinkGraph


class HITS:

    def __init__(self) -> None:
        self.a_entropy: List[float] = []
        self.h_entropy: List[float] = []

    @classmethod
    def _update_authority_score(cls, urls: Set[str], hub_scores: Dict[str, float],
                                linkgraph: LinkGraph) -> Dict[str, float]:
        authority_scores = defaultdict(lambda: 0)
        for p in urls:
            for q in linkgraph.get_inlinks(p):
                authority_scores[p] += hub_scores.get(q, 1)

        return authority_scores

    @classmethod
    def _update_hub_score(cls, urls: Set[str], authority_scores: Dict[str, float],
                          linkgraph: LinkGraph) -> Dict[str, float]:
        hub_scores = defaultdict(lambda: 0)
        for p in urls:
            for q in linkgraph.get_outlinks(p):
                hub_scores[p] += authority_scores.get(q, 1)

        return hub_scores

    @classmethod
    def _normalize_scores(cls, scores: Dict[str, float]) -> Dict[str, float]:
        denominator = math.sqrt(sum([x * x for x in scores.values()]))
        return {url: score / denominator for url, score in scores.items()}

    @classmethod
    def _calculate_entropy(cls, scores: Dict[str, float]) -> float:
        entropy = 0.0
        for url, score in scores.items():
            entropy += score * math.log2(score)

        entropy = -entropy
        return entropy

    def _has_converged(self, authority_scores: Dict[str, float],
                       hub_scores: Dict[str, float]) -> Tuple[float, bool, float, bool]:

        def _converge_helper(perplexity: List[float]):
            _converged = False
            if len(perplexity) == 4:
                _converged = True
                for i in range(3):
                    if abs(perplexity[i] - perplexity[i + 1]) > 0.001:
                        _converged = False
                        break

                del perplexity[0]

            return _converged

        a_entropy = self._calculate_entropy(authority_scores)
        h_entropy = self._calculate_entropy(hub_scores)
        self.a_entropy.append(a_entropy)
        self.h_entropy.append(h_entropy)
        a_converged = _converge_helper(self.a_entropy)
        h_converged = _converge_helper(self.h_entropy)

        return a_entropy, a_converged, h_entropy, h_converged

    def calculate_hub_and_authority_score(self, linkgraph: LinkGraph, root_set: Set[str]) -> Tuple[Dict[str, float],
                                                                                                   Dict[str, float]]:

        authority_scores = defaultdict(lambda: 1)
        hub_scores = defaultdict(lambda: 1)

        i = 1
        while True:
            a_entropy, a_converged, h_entropy, h_converged = self._has_converged(authority_scores, hub_scores)
            logging.info(
                "Iteration:{}, A_Entropy:{}, A_Converged:{}, H_Entropy:{}, H_Converged:{}".format(i, a_entropy,
                                                                                                  a_converged,
                                                                                                  h_entropy,
                                                                                                  h_converged))

            if a_converged and h_converged:
                break

            new_authority_score = self._update_authority_score(root_set, hub_scores, linkgraph)
            new_hub_score = self._update_hub_score(root_set, authority_scores, linkgraph)
            authority_scores = self._normalize_scores(new_authority_score)
            hub_scores = self._normalize_scores(new_hub_score)

            i += 1

        self.a_entropy = []
        return authority_scores, hub_scores

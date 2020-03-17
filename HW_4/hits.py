import logging
import math
from collections import defaultdict
from typing import Tuple, Dict, Set, List

from utils.utils import LinkGraph


class HITS:

    def __init__(self) -> None:
        self.a_perplexity: List[float] = []
        self.h_perplexity: List[float] = []

    @classmethod
    def _get_default_authority_scores(cls) -> Dict[str, float]:
        return defaultdict(lambda: 1)

    @classmethod
    def _get_default_hub_scores(cls) -> Dict[str, float]:
        return defaultdict(lambda: 1)

    def _update_authority_score(self, urls: Set[str], hub_scores: Dict[str, float],
                                linkgraph: LinkGraph) -> Dict[str, float]:
        authority_scores = self._get_default_authority_scores()
        for p in urls:
            for q in linkgraph.get_inlinks(p):
                authority_scores[p] += hub_scores[q]

        return authority_scores

    def _update_hub_score(self, urls: Set[str], authority_scores: Dict[str, float],
                          linkgraph: LinkGraph) -> Dict[str, float]:
        hub_scores = self._get_default_hub_scores()
        for p in urls:
            for q in linkgraph.get_outlinks(p):
                hub_scores[p] += authority_scores[q]

        return hub_scores

    @classmethod
    def _normalize_scores(cls, scores: Dict[str, float]) -> Dict[str, float]:
        denominator = math.sqrt(sum([x * x for x in scores.values()]))
        return {url: score / denominator for url, score in scores.items()}

    @classmethod
    def _calculate_perplexity(cls, scores: Dict[str, float]) -> float:
        entropy = 0.0
        for url, score in scores.items():
            entropy += score * math.log2(score)

        entropy = -entropy
        return 2 ** entropy

    def _has_converged(self, authority_scores: Dict[str, float],
                       hub_scores: Dict[str, float]) -> Tuple[float, bool, float, bool]:

        def _converge_helper(perplexity: List[float]):
            _converged = False
            if len(perplexity) == 4:
                _converged = True
                for i in range(3):
                    if int(perplexity[i]) != int(perplexity[i + 1]):
                        _converged = False
                        break

                del perplexity[0]

            return _converged

        a_perplexity = self._calculate_perplexity(authority_scores)
        h_perplexity = self._calculate_perplexity(hub_scores)
        self.a_perplexity.append(a_perplexity)
        self.h_perplexity.append(h_perplexity)
        a_converged = _converge_helper(self.a_perplexity)
        h_converged = _converge_helper(self.h_perplexity)

        return a_perplexity, a_converged, h_perplexity, h_converged

    def calculate_hub_and_authority_score(self, linkgraph: LinkGraph, root_set: Set[str]) -> Tuple[Dict[str, float],
                                                                                                   Dict[str, float]]:

        authority_scores = self._get_default_authority_scores()
        hub_scores = self._get_default_hub_scores()

        i = 1
        while True:
            a_perplexity, a_converged, h_perplexity, h_converged, = self._has_converged(authority_scores, hub_scores)
            logging.info(
                "Iteration:{}, A_Perplexity:{}, A_Converged:{}, H_Perplexity:{}, H_Converged:{}".format(i, a_perplexity,
                                                                                                        a_converged,
                                                                                                        h_perplexity,
                                                                                                        h_converged))

            if a_converged and h_converged:
                break

            new_authority_score = self._update_authority_score(root_set, hub_scores, linkgraph)
            new_hub_score = self._update_hub_score(root_set, authority_scores, linkgraph)
            authority_scores = self._normalize_scores(new_authority_score)
            hub_scores = self._normalize_scores(new_hub_score)

            i += 1

        self.a_perplexity = []
        return authority_scores, hub_scores

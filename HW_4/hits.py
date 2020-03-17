from collections import defaultdict
from typing import Tuple, Dict, Set

from utils.utils import LinkGraph


class HITS:

    @classmethod
    def _update_hub_score(cls, urls: Set[str], authority_score: Dict[str, float], linkgraph: LinkGraph) -> Dict[
        str, float]:
        pass

    def calculate_hub_and_authority_score(self, linkgraph: LinkGraph, root_set: Set[str]) -> Tuple[Dict[str, float],
                                                                                                   Dict[str, float]]:
        hub_score = defaultdict()
        initial_hub_score = 1
        authority_score = {}
        initial_authority_score = 1

        while True:
            pass

        return authority_score, hub_score

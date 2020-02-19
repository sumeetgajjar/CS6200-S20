import csv
import json
import logging
import os
import sys

from CS6200_S20_SHARED.url_cleaner import UrlCleaner
from utils.decorators import timing
from utils.utils import Utils


class DomainRank:

    def __init__(self, domain, global_rank, tld_rank) -> None:
        self.domain = domain
        self.global_rank = global_rank
        self.tld_rank = tld_rank

    def __str__(self) -> str:
        return "Domain:{}, GlobalRank:{}, TldRank:{}".format(self.domain, self.global_rank, self.tld_rank)


class DomainRankings:
    _DEFAULT_DOMAIN_RANK = sys.maxsize
    _RAW_DOMAIN_RANK_FILE_PATH = '/home/sumeet/PycharmProjects/CS6200-S20/CS6200_S20_SHARED/domain_rankings.csv'
    _PROCESSED_DOMAIN_RANK_FILE_NAME = 'processed_domain_ranking.json'
    _PROCESSED_DOMAIN_RANK_FILE_PATH = '{}/{}'.format(Utils.get_data_dir_abs_path(), _PROCESSED_DOMAIN_RANK_FILE_NAME)

    def __init__(self) -> None:
        self.domain_rankings = {}
        self._init_domain_rankings()

    @timing
    def _init_domain_rankings(self):
        if os.path.isfile(self._PROCESSED_DOMAIN_RANK_FILE_PATH):
            logging.info("Processed Domain rank exists")
            logging.info("Reading the processed domain ranks from json")
            with open(self._PROCESSED_DOMAIN_RANK_FILE_PATH, 'r') as file:
                self.domain_rankings = json.load(file)
        else:
            logging.info("Processing Domain ranks")
            self._read_domain_ranks()

            logging.info("Writing processed domain ranks to json")
            with open(self._PROCESSED_DOMAIN_RANK_FILE_PATH, 'w') as file:
                json.dump(self.domain_rankings, file)

            logging.info("Domain ranks processed")

    @timing
    def _read_domain_ranks(self):
        with open(self._RAW_DOMAIN_RANK_FILE_PATH, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                domain = UrlCleaner.get_domain_from_url(row['Domain'])
                domain = row['Domain']
                self.domain_rankings[domain] = [row['GlobalRank'], row['TldRank']]

    def get_domain_rank(self, canonical_domain: str) -> DomainRank:
        """
        :param canonical_domain:
        :return: the domain rank
        """
        domain_rank_info = self.domain_rankings.get(canonical_domain)
        if domain_rank_info:
            return DomainRank(canonical_domain, domain_rank_info[0], domain_rank_info[1])
        else:
            return DomainRank(canonical_domain, self._DEFAULT_DOMAIN_RANK, self._DEFAULT_DOMAIN_RANK)


if __name__ == '__main__':
    Utils.configure_logging()
    print(DomainRankings().get_domain_rank('wikipedia.org'))

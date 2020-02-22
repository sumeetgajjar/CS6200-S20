import pytest

from CS6200_S20_SHARED.url_cleaner import UrlCleaner
from HW_3.factory import Factory
from HW_3.filter import DomainRanker, CrawlingUtils
from utils.utils import Utils


@pytest.fixture(scope='module')
def domain_ranker():
    return DomainRanker()


@pytest.fixture(scope='module')
def url_cleaner():
    return Factory.create_url_cleaner()


def test_domain_ranker(domain_ranker: DomainRanker):
    domain_rank = domain_ranker.get_domain_rank('wikipedia.org')
    assert domain_rank.global_rank == 9
    assert domain_rank.tld_rank == 1

    domain_rank = domain_ranker.get_domain_rank('facebook.com')
    assert domain_rank.global_rank == 1
    assert domain_rank.tld_rank == 1

    domain_rank = domain_ranker.get_domain_rank('el.wikipedia.org')
    assert domain_rank.global_rank == 12127
    assert domain_rank.tld_rank == 1276


def test_crawling_utils(url_cleaner: UrlCleaner):
    urls = ['https://dummy-domain-for-unitesting.com', 'http://dummy-domain-for-unitesting.com', '123.abcd.com']
    url_details = [url_cleaner.get_canonical_url(url) for url in urls]
    CrawlingUtils.add_urls_to_crawled_list(url_details)

    for url_detail in url_details:
        assert CrawlingUtils.is_crawled(url_detail)


if __name__ == '__main__':
    Utils.configure_logging()
    pytest.main()

import pytest

from HW_3.filter import DomainRanker
from utils.utils import Utils


@pytest.fixture(scope='module')
def domain_ranker():
    return DomainRanker()


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


if __name__ == '__main__':
    Utils.configure_logging()
    pytest.main()

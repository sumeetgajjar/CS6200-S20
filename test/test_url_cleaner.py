import pytest

from HW_3.url_cleaner import UrlCleaner


@pytest.fixture(scope='module')
def url_cleaner():
    return UrlCleaner()


def test_url_strips(url_cleaner: UrlCleaner):
    url = 'http://www.google.com/   '
    assert url_cleaner.get_canonical_url(url) == 'http://google.com', "Url not stripped"


if __name__ == '__main__':
    pytest.main()

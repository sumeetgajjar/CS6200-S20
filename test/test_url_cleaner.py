import pytest

from HW_3.url_cleaner import UrlCleaner


@pytest.fixture(scope='module')
def url_cleaner():
    return UrlCleaner()


@pytest.mark.parametrize(
    'url, canonical_url, error_msg',
    [
        pytest.param('www.google.com/asd.html   ', 'http://google.com/asd.html', 'url not stripped'),
        pytest.param('http://www.google.com   ', 'http://google.com', 'url not stripped'),
        pytest.param('www.google.com/asd.html?PHPSESSID=1234413123&p1=v1&p2=v2',
                     'http://google.com/asd.html?&p1=v1&p2=v2',
                     'session id not removed'),
        pytest.param('HTTP://www.google.com/asd.html   ', 'http://google.com/asd.html', 'scheme is not lowercase'),
        pytest.param('HTTP://www.gOogle.com/asd.html   ', 'http://google.com/asd.html',
                     'scheme or host is not lowercase'),
        pytest.param('http://www.google.com:80/asd.html', 'http://google.com/asd.html', 'http port not removed'),
        pytest.param('https://www.google.com:443/asd.html', 'https://google.com/asd.html', 'https port not removed'),
        pytest.param('http://www.google.com:1243/index.asp?p1=v1', 'http://google.com:1243?p1=v1',
                     'non default port stripped'),
        pytest.param('http://www.google.com/asd.html/  ?p1=v1', 'http://google.com/asd.html/?p1=v1',
                     'path not stripped'),
        pytest.param('http://www.google.com/asd.html/?   p1=v1', 'http://google.com/asd.html/?p1=v1',
                     'query not stripped'),
        pytest.param('http://www.google.com/index.html?p1=v1', 'http://google.com?p1=v1', 'index.html not stripped'),
        pytest.param('http://www.google.com/index.html?p1=v1', 'http://google.com?p1=v1', 'index.html not stripped'),
        pytest.param('http://www.google.com/index.htm?p1=v1', 'http://google.com?p1=v1', 'index.htm not stripped'),
        pytest.param('http://www.google.com/index.php?p1=v1', 'http://google.com?p1=v1', 'index.php not stripped'),
        pytest.param('http://www.google.com/index.asp?p1=v1', 'http://google.com?p1=v1', 'index.asp not stripped'),
        pytest.param('http://www.google.com/default.aspx?p1=v1', 'http://google.com?p1=v1',
                     'default.aspx not stripped'),
        pytest.param('http://www.google.com///asd/ad/////asd/////index.html?p1=v1#123',
                     'http://google.com/asd/ad/asd/?p1=v1', 'duplicate slash not removed')
    ]
)
def test_get_canonical_url(url: str, canonical_url: str, error_msg: str, url_cleaner: UrlCleaner):
    assert url_cleaner.get_canonical_url(url).geturl() == canonical_url, error_msg


@pytest.mark.xfail(raises=ValueError)
def test_invalid_host_failure(url_cleaner: UrlCleaner):
    url_cleaner.get_canonical_url("http://www.google.om:123:123/asd.html")


if __name__ == '__main__':
    pytest.main()

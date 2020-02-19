import re
from urllib.parse import urlparse, ParseResult


class UrlCleaner:
    # Url Cleaner Constants
    _DEFAULT_FILE_NAME_TO_REMOVE_LIST = ["\\index.html", "\\index.php", "\\default.aspx", "\\index.asp", "\\index.htm"]
    _SESSION_IDS_TO_REMOVE = "PHPSESSID|JSESSIONID|ASPSESSIONID|ZENID|phpsessid|jsessionid|aspsessionid|zenid"
    _SESSION_IDS_TO_REMOVE_REGEX = re.compile("([&?;])({})=[^&]*".format(_SESSION_IDS_TO_REMOVE), re.IGNORECASE)
    _ESCAPE_SEQUENCES_REGEX = re.compile("([\n\t\r])", re.IGNORECASE)
    _SLASH_REGEX = re.compile("\\\\", re.IGNORECASE)
    _TRAILING_SPECIAL_CHARS_REGEX = re.compile("([&?])*$", re.IGNORECASE)
    _DUPLICATE_SLASH_REGEX = re.compile("/{2,}", re.IGNORECASE)

    @classmethod
    def _remove_port_if_exists(cls, netloc: str) -> str:
        return netloc.split(":")[0]

    @classmethod
    def _remove_www(cls, netloc: str) -> str:
        if netloc.startswith("www."):
            return netloc[4:]
        else:
            return netloc

    def _remove_trailing_tld_slash(self, parsed_url: ParseResult) -> ParseResult:
        if not parsed_url.query and parsed_url.path == '/':
            return parsed_url._replace(path='')
        else:
            return parsed_url

    def _remove_trailing_default_file_names(self, parsed_url: ParseResult) -> ParseResult:
        for default_file_name in self._DEFAULT_FILE_NAME_TO_REMOVE_LIST:
            if parsed_url.path.startswith(default_file_name):
                return parsed_url._replace(path=parsed_url.path[len(default_file_name):])

        return parsed_url

    def _clean_scheme_host_and_fragment(self, url: str) -> ParseResult:
        parsed_url = urlparse(url)
        lowercase_scheme = parsed_url.scheme.lower()

        lowercase_netloc = parsed_url.netloc.strip().lower()
        port_removed_netloc = self._remove_port_if_exists(lowercase_netloc)
        www_removed_netloc = self._remove_www(port_removed_netloc)

        stripped_query = parsed_url.query.strip()
        stripped_path = parsed_url.path.strip()

        parsed_url = parsed_url._replace(scheme=lowercase_scheme,
                                         netloc=www_removed_netloc,
                                         query=stripped_query,
                                         path=stripped_path,
                                         fragment='')

        return parsed_url

    def _remove_escape_sequences(self, url: str) -> str:
        return self._SLASH_REGEX.sub("", self._ESCAPE_SEQUENCES_REGEX.sub("", url))

    def _remove_session_ids(self, url: str) -> str:
        return self._SESSION_IDS_TO_REMOVE_REGEX.sub("", url)

    def _remove_trailing_special_chars(self, url: str) -> str:
        return self._TRAILING_SPECIAL_CHARS_REGEX.sub("", url)

    def _remove_duplicate_slashes(self, url: str) -> str:
        return self._DUPLICATE_SLASH_REGEX.sub("/", url)

    def get_canonical_url(self, url: str) -> str:
        url = url.strip()
        url = self._remove_escape_sequences(url)
        url = self._remove_session_ids(url)
        url = self._remove_duplicate_slashes(url)
        parsed_url = self._clean_scheme_host_and_fragment(url)
        parsed_url = self._remove_trailing_default_file_names(parsed_url)
        parsed_url = self._remove_trailing_tld_slash(parsed_url)

        url = parsed_url.geturl()

        return url

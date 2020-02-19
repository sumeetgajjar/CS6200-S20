import re
from urllib.parse import urlparse, ParseResult, urljoin


class UrlCleaner:
    # Url Cleaner Constants
    _DEFAULT_FILE_NAME_TO_REMOVE_LIST = ["index.html", "index.php", "default.aspx", "index.asp", "index.htm"]
    _SESSION_IDS_TO_REMOVE = "PHPSESSID|JSESSIONID|ASPSESSIONID|ZENID|phpsessid|jsessionid|aspsessionid|zenid"
    _SESSION_IDS_TO_REMOVE_REGEX = re.compile("([&?;]{,1})(%s)=[^&]*" % _SESSION_IDS_TO_REMOVE, re.IGNORECASE)
    _ESCAPE_SEQUENCES_REGEX = re.compile("([\n\t\r])", re.IGNORECASE)
    _SLASH_REGEX = re.compile("\\\\", re.IGNORECASE)
    _TRAILING_SPECIAL_CHARS_REGEX = re.compile("([&?])*$", re.IGNORECASE)
    _DUPLICATE_SLASH_REGEX = re.compile("/{2,}", re.IGNORECASE)

    @classmethod
    def _remove_port_if_exists(cls, scheme: str, netloc: str) -> str:
        splits = netloc.split(":")
        if len(splits) == 1:
            return netloc
        elif len(splits) == 2:
            port = splits[1]
            if scheme == 'http' and port == '80':
                return splits[0]
            elif scheme == 'https' and port == '443':
                return splits[0]
            else:
                return netloc
        else:
            raise ValueError("Invalid Domain: {}".format(netloc))

    @classmethod
    def _remove_www(cls, netloc: str) -> str:
        if netloc.startswith("www."):
            return netloc[4:]
        else:
            return netloc

    def _remove_trailing_tld_slash(self, parsed_url: ParseResult) -> ParseResult:
        if parsed_url.path == '/':
            return parsed_url._replace(path='')
        else:
            return parsed_url

    def _remove_trailing_default_file_names(self, parsed_url: ParseResult) -> ParseResult:
        for default_file_name in self._DEFAULT_FILE_NAME_TO_REMOVE_LIST:
            if parsed_url.path.endswith(default_file_name):
                return parsed_url._replace(path=parsed_url.path[:len(parsed_url.path) - len(default_file_name)])

        return parsed_url

    def _clean_scheme_host_and_fragment(self, url: str) -> ParseResult:
        parsed_url = urlparse(url)
        lowercase_scheme = parsed_url.scheme.lower()

        lowercase_netloc = parsed_url.netloc.strip().lower()
        port_removed_netloc = self._remove_port_if_exists(lowercase_scheme, lowercase_netloc)
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

    def _remove_session_ids(self, parsed_url: ParseResult) -> ParseResult:
        return parsed_url._replace(query=self._SESSION_IDS_TO_REMOVE_REGEX.sub("", parsed_url.query))

    def _remove_trailing_special_chars(self, url: str) -> str:
        return self._TRAILING_SPECIAL_CHARS_REGEX.sub("", url)

    def _remove_duplicate_slashes(self, parsed_url: ParseResult) -> ParseResult:
        return parsed_url._replace(path=self._DUPLICATE_SLASH_REGEX.sub("/", parsed_url.path))

    @classmethod
    def _add_protocol_if_not_exists(cls, url: str) -> str:
        if url.lower().startswith("http"):
            return url
        else:
            return 'http://{}'.format(url)

    def get_canonical_url(self, url: str) -> ParseResult:
        """
        It applies the following rules to the url in the given order
        1. strips the url
        2. removes all the escape sequences from the url
        3. adds http as the protocol if not present
        4. lower case the scheme
        5. remove the default http and https port if exists
        6. remove www from the hostname
        7. strip the query
        8. strip the path
        9. remove the fragment
        10. remove session ids
        11. remove default file name like index.html etc
        12. remove duplicate slashes
        13. remove trailing tld slash

        :param url:
        :return: ParseResult which contains canonical form of the given url
        """
        url = url.strip()
        url = self._remove_escape_sequences(url)
        url = self._add_protocol_if_not_exists(url)
        parsed_url = self._clean_scheme_host_and_fragment(url)
        parsed_url = self._remove_session_ids(parsed_url)
        parsed_url = self._remove_trailing_default_file_names(parsed_url)
        parsed_url = self._remove_duplicate_slashes(parsed_url)
        parsed_url = self._remove_trailing_tld_slash(parsed_url)

        return parsed_url

    def transform_relative_url_to_absolute_url(self, current_url: str, relative_url: str) -> ParseResult:
        """
        :param current_url:
        :param relative_url:
        :return: ParseResult which contains the canonical form of the absolute url of the given relative url
        """
        return self.get_canonical_url(urljoin(current_url, relative_url))

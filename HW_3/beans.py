from typing import Optional, Set

from CS6200_S20_SHARED.url_cleaner import UrlDetail


class DomainRank:

    def __init__(self, domain, global_rank, tld_rank) -> None:
        self.domain = domain
        self.global_rank = global_rank
        self.tld_rank = tld_rank

    def __str__(self) -> str:
        return "Domain:{}, GlobalRank:{}, TldRank:{}".format(self.domain, self.global_rank, self.tld_rank)

    def __repr__(self):
        return self.__str__()


class Outlink:

    def __init__(self, url_detail: UrlDetail, anchor_text: str) -> None:
        self.url_detail: UrlDetail = url_detail
        self.anchor_text: str = anchor_text.strip().lower() if anchor_text else ''

    def __str__(self) -> str:
        return 'Anchor Text:{}, UrlDetail:{}'.format(self.anchor_text, self.url_detail)

    def __repr__(self):
        return self.__str__()


class FilteredResult:

    def __init__(self, filtered, removed) -> None:
        self.filtered = filtered
        self.removed = removed

    def __str__(self) -> str:
        return 'Filtered:{}, Removed:{}'.format(self.filtered, self.removed)

    def __repr__(self):
        return self.__str__()


class CrawlerResponse:

    def __init__(self, url_detail) -> None:
        self.url_detail: UrlDetail = url_detail
        self.raw_html: Optional[str] = None
        self.headers: Optional[dict] = None
        self.is_redirected: bool = False
        self.redirected_url: Optional[UrlDetail] = None
        self.meta_keywords: Set[str] = set()
        self.meta_description: Optional[str] = None

    def __str__(self) -> str:
        return 'UrlDetail:{}, Header:{}, RedirectedUrlDetail:{}'.format(self.url_detail, self.headers,
                                                                        self.redirected_url)

    def __repr__(self):
        return self.__str__()

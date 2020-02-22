from typing import Optional

from CS6200_S20_SHARED.url_cleaner import UrlDetail


class DomainRank:

    def __init__(self, domain, global_rank, tld_rank) -> None:
        self.domain = domain
        self.global_rank = global_rank
        self.tld_rank = tld_rank

    def __str__(self) -> str:
        return "Domain:{}, GlobalRank:{}, TldRank:{}".format(self.domain, self.global_rank, self.tld_rank)


class Outlink:

    def __init__(self, url_detail: UrlDetail, anchor_text: str) -> None:
        self.url_detail: UrlDetail = url_detail
        self.anchor_text: str = anchor_text


class FilteredResult:

    def __init__(self, filtered, removed) -> None:
        self.filtered = filtered
        self.removed = removed


class CrawlerResponse:

    def __init__(self, url_detail) -> None:
        self.url_detail: UrlDetail = url_detail
        self.raw_html: Optional[str] = None
        self.headers: Optional[dict] = None
        self.redirected: bool = False
        self.redirected_url: Optional[UrlDetail] = None
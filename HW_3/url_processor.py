from typing import List, Tuple

from bs4 import BeautifulSoup

from constants.constants import Constants


class UrlProcessor:

    @classmethod
    def _clean_html(cls, soup: BeautifulSoup):
        for tag_to_remove in Constants.TAGS_TO_REMOVE:
            for element in soup.find_all(tag_to_remove):
                element.clear()

    @classmethod
    def _extract_outlinks(cls, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        return [(a_element['href'], a_element.text) for a_element in soup.find_all('a') if a_element['href']]

    # def start(self):

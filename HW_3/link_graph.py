import logging
from typing import List

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from HW_3.beans import Outlink
from constants.constants import Constants
from utils.utils import Utils


class LinkGraph:

    @classmethod
    def _insert_edges_to_mysql(cls, edges_xml: str):
        with Constants.MYSQL_ENGINE.connect() as conn:
            result = conn.execute('call sp_insert_link_graph_edges(@var_edges_xml:=%s)', [edges_xml])
            logging.info("Added {} edge(s) to the link graph".format(result.rowcount))

    @classmethod
    def _generate_edges_xml(cls, src: UrlDetail, dests: List[UrlDetail]) -> str:
        rows = []
        for dest in dests:
            rows.append('<r><s><![CDATA[{}]]></s><d><![CDATA[{}]]></d></r>'.format(src.canonical_url,
                                                                                   dest.canonical_url))
        return "<rt>{}</rt>".format("".join(rows))

    @classmethod
    def add_edge(cls, src: UrlDetail, destination: UrlDetail):
        cls.add_edges(src, [Outlink(destination, "")])

    @classmethod
    def add_edges(cls, src: UrlDetail, outlinks: List[Outlink]):
        edges_xml = cls._generate_edges_xml(src, [outlink.url_detail for outlink in outlinks])
        cls._insert_edges_to_mysql(edges_xml)


if __name__ == '__main__':
    Utils.configure_logging()
    LinkGraph._insert_edges_to_mysql('<rt><r><s><![CDATA[sumeet-212]]></s><d><![CDATA[sumeet-2]]></d></r></rt>')

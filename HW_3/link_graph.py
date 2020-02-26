import logging
from typing import List

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from HW_3.beans import Outlink
from HW_3.factory import Factory
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
        urls = [outlink.url_detail for outlink in outlinks]
        for urls_batch in Utils.split_list_into_sub_lists(urls, sub_list_size=Constants.LINK_GRAPH_INSERT_BATCH_SIZE):
            edges_xml = cls._generate_edges_xml(src, urls_batch)
            cls._insert_edges_to_mysql(edges_xml)


if __name__ == '__main__':
    Utils.configure_logging()
    LinkGraph.add_edge(Factory.create_url_cleaner().get_canonical_url('testing-src-6200.com'),
                       Factory.create_url_cleaner().get_canonical_url('testing-dest-6200.com'))

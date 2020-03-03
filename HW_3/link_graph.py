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
        with Constants.MYSQL_ENGINE.connect() as conn:
            result = conn.execute("""
                                  insert into cs6200.link_graph_edges(src, src_hash, dest, dest_hash)
                                  values (%s, %s, %s, %s) 
                                  """, [(src.canonical_url, src.id, o.url_detail.canonical_url, o.url_detail.id)
                                        for o in outlinks])
            logging.info("Added {} edge(s) to the link graph".format(result.rowcount))


if __name__ == '__main__':
    Utils.configure_logging()
    LinkGraph.add_edge(Factory.create_url_cleaner().get_canonical_url('testing-src-6200.com'),
                       Factory.create_url_cleaner().get_canonical_url('testing-dest-6200.com'))

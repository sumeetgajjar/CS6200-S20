import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from HW_1.constants import Constants
from utils.decorators import timing

ES_HOSTS = [{"host": "localhost", "port": 9200}]
ES_CLIENT = None


class EsUtils:

    @classmethod
    def get_es_client(cls):
        global ES_CLIENT
        if not ES_CLIENT:
            logging.info("Creating the ES client")
            ES_CLIENT = Elasticsearch(hosts=ES_HOSTS)
            logging.info("ES client created")

        return ES_CLIENT

    @classmethod
    def create_es_index(cls, name, index_config):
        logging.info('Creating "{}" index'.format(name))
        es = cls.get_es_client()
        response = es.indices.create(index=name, body=index_config)
        logging.info(response)
        logging.info('"{}" index created'.format(name))

    @classmethod
    def delete_es_index(cls, name, ignore_unavailable):
        logging.info('Deleting "{}" index'.format(name))
        es_client = cls.get_es_client()
        response = es_client.indices.delete(index=name, ignore_unavailable=ignore_unavailable)
        logging.info(response)
        logging.info('"{}" index deleted'.format(name))

    @classmethod
    @timing
    def bulk_add_document_to_ap_data_index(cls, documents: list, chunk_size=10000):
        def get_document_generator():
            for document in documents:
                yield {
                    '_index': Constants.AP_DATA_INDEX_NAME,
                    '_id': document['id'],
                    '_source': {
                        'text': document['text'],
                        'length': document['length']
                    }
                }

        es_client = cls.get_es_client()
        response = bulk(client=es_client, actions=get_document_generator(), chunk_size=chunk_size)
        logging.info(response)

    def get_all_document_ids(cls, index_name: str) -> list:
        pass
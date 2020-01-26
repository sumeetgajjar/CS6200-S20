import logging

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk, scan

from HW_1.constants import Constants
from utils.decorators import timing

ES_HOSTS = [{"host": "localhost", "port": 9200}]
ES_CLIENT = None


class EsUtils:

    @classmethod
    def get_es_client(cls, timeout: int = Constants.TIMEOUT):
        global ES_CLIENT
        if True:  # not ES_CLIENT:
            logging.info("Creating the ES client")
            ES_CLIENT = Elasticsearch(hosts=ES_HOSTS, send_get_body_as='POST', timeout=timeout)
            logging.info("ES client created")

        return ES_CLIENT

    @classmethod
    def get_indices_client(cls):
        logging.info("Creating ES Indices Client")
        client = IndicesClient(cls.get_es_client())
        logging.info("Indices Client created")
        return client

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
    def bulk_add_document_to_ap_data_index(cls, documents: list, chunk_size: int = Constants.CHUNK_SIZE):
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

    @classmethod
    def get_match_all_query(cls):
        return {
            "query": {
                "match_all": {}
            },
            "stored_fields": []
        }

    @classmethod
    @timing
    def get_all_document_ids(cls, index_name: str, chunk_size: int = Constants.CHUNK_SIZE) -> list:
        es_client = cls.get_es_client()
        response = scan(es_client, query=cls.get_match_all_query(), index=index_name, size=chunk_size)
        document_ids = [result['_id'] for result in response]
        logging.info("{} total docs in index: {}".format(len(document_ids), index_name))
        return document_ids

    @classmethod
    def get_mtermvector_query(cls, document_ids: list):
        return {
            "ids": document_ids,
            "parameters": {
                "fields": ["text"],
                "term_statistics": True,
                "positions": False,
                "offsets": False,
                "field_statistics": False,
                "payloads": False
            }
        }

    @classmethod
    @timing
    def get_termvectors(cls, index_name: str, document_ids: list, timeout: int = Constants.TIMEOUT) -> dict:
        es_client = cls.get_es_client(timeout)
        response = es_client.mtermvectors(index=index_name, body=cls.get_mtermvector_query(document_ids))
        return {
            doc['_id']: doc for doc in response['docs']
        }

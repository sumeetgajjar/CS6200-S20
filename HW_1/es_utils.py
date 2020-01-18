import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from HW_1.Constants import AP_DATA_INDEX_NAME

ES_HOSTS = [{"host": "localhost", "port": 9200}]
ES_CLIENT = None


def get_es_client():
    global ES_CLIENT
    if not ES_CLIENT:
        logging.info("Creating the ES client")
        ES_CLIENT = Elasticsearch(hosts=ES_HOSTS)
        logging.info("ES client created")

    return ES_CLIENT


def create_es_index(name, index_config):
    logging.info('Creating "{}" index'.format(name))
    es = get_es_client()
    response = es.indices.create(index=name, body=index_config)
    logging.info(response)
    logging.info('"{}" index created'.format(name))


def delete_es_index(name, ignore_unavailable):
    logging.info('Deleting "{}" index'.format(name))
    es_client = get_es_client()
    response = es_client.indices.delete(index=name, ignore_unavailable=ignore_unavailable)
    logging.info(response)
    logging.info('"{}" index deleted'.format(name))


def bulk_add_document_to_ap_data_index(documents: list):
    def get_document_generator():
        for document in documents:
            yield {
                '_index': AP_DATA_INDEX_NAME,
                '_id': document['id'],
                '_source': {
                    'text': document['text'],
                    'length': document['length']
                }
            }

    es_client = get_es_client()
    response = bulk(client=es_client, actions=get_document_generator())
    logging.info(response)

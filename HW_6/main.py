import logging
from typing import Set

from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from utils.utils import Utils


class HW6:

    @classmethod
    def _get_doc_ids(cls) -> Set[str]:
        logging.info('Parsing QREL for doc ids')
        qrel_file_path = '{}/{}'.format(Utils.get_ap_data_path(), 'qrels.adhoc.51-100.AP89.txt')
        doc_ids = set()
        with open(qrel_file_path, 'r') as file:
            for line in file:
                parts = line.strip().split(" ")
                doc_id = parts[2].strip()
                doc_ids.add(doc_id)

        logging.info("QREL file parsed. {} unique docs found".format(len(doc_ids)))
        return doc_ids

    @classmethod
    def _read_qrel_documents(cls):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        parsed_documents = get_parsed_documents(file_paths)

        qrel_docs_ids = cls._get_doc_ids()
        documents = {doc['id']: doc for doc in parsed_documents if doc['id'] in qrel_docs_ids}

        assert len(qrel_docs_ids) == len(documents), "Not all QREL documents were read"
        return documents

    @classmethod
    def main(cls):
        print(len(cls._read_qrel_documents()))


if __name__ == '__main__':
    Utils.configure_logging()
    HW6.main()

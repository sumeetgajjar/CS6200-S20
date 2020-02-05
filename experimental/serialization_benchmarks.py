import json
import pickle

from HW_2.termvector import TermVector, TfInfo
from protos import termvector_pb2
from utils.decorators import timing
from utils.utils import Utils


@timing
def test_protos():
    termvector = termvector_pb2.TermVector()
    termvector.term = 'test'

    for j in range(100):
        for i in range(10000):
            doc_id = 'doc-id-{}'.format(j)
            termvector.tfInfo[doc_id].tf += 1
            termvector.tfInfo[doc_id].positions.append(i)
            termvector.ttf += 1

    print(len(termvector.SerializeToString()))


@timing
def test_objects():
    termvector = TermVector('test')

    for j in range(100):
        doc_id = 'doc-id-{}'.format(j)
        tf_info = TfInfo()
        for i in range(10000):
            tf_info.tf += 1
            tf_info.positions.append(i)
            termvector.ttf += 1
        termvector.tfInfo[doc_id] = tf_info

    print(len(pickle.dumps(termvector)))


@timing
def test_jsons():
    termvector = {'term': 'test', 'tf_info': {}, 'ttf': 0}

    for j in range(100):
        doc_id = 'doc-id-{}'.format(j)
        tf_info = {'tf': 0, 'pos': []}
        for i in range(10000):
            tf_info['tf'] += 1
            tf_info['pos'].append(i)
            termvector['ttf'] += 1
        termvector['tf_info'][doc_id] = tf_info

    print(len(json.dumps(termvector)))


if __name__ == '__main__':
    Utils.configure_logging()
    test_protos()
    test_objects()
    test_jsons()

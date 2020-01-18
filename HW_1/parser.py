import logging
import re


class TRECParser:
    def __init__(self, file_encoding='latin-1') -> None:
        self.file_encoding = file_encoding
        self.doc = {'start': '<DOC>', 'end': '</DOC>'}
        self.doc_no = {'start': '<DOCNO>', 'end': '</DOCNO>'}
        self.text = {'start': '<TEXT>', 'end': '</TEXT>'}
        self.doc_no_regex = re.compile('<DOCNO>(.*)</DOCNO>')

    def _is_tag_start(self, tag: str, line: str) -> bool:
        return line.startswith(getattr(self, tag)['start'])

    def _is_tag_end(self, tag: str, line: str) -> bool:
        return line.rstrip().endswith(getattr(self, tag)['end'])

    def is_doc_start(self, line: str) -> bool:
        return self._is_tag_start('doc', line)

    def is_doc_end(self, line: str) -> bool:
        return self._is_tag_end('doc', line)

    def is_doc_no_start(self, line: str) -> bool:
        return self._is_tag_start('doc_no', line)

    def is_doc_no_end(self, line: str) -> bool:
        return self._is_tag_end('doc_no', line)

    def is_text_start(self, line: str) -> bool:
        return self._is_tag_start('text', line)

    def is_text_end(self, line: str) -> bool:
        return self._is_tag_end('text', line)

    def _parse_doc_no(self, line, document):
        if not self.is_doc_no_end(line):
            raise RuntimeError('line does not end with doc_no end tag')

        doc_no = self.doc_no_regex.match(line).group(1)
        doc_no = doc_no.strip()
        if not doc_no:
            raise RuntimeError('Empty doc_no in line: {}'.format(line))
        document['doc_no'] = doc_no

    def _parse_text(self, file, document):
        lines = []
        while True:
            line = file.readline()
            if not line:
                raise RuntimeError('unexpected EOF while parsing text')

            if self.is_text_end(line):
                text = ''.join(lines)
                document['text'] = text
                return

            lines.append(line)

    @staticmethod
    def _document_sanity_check(document):
        for attr in ['doc_no', 'text']:
            if attr not in document:
                raise RuntimeError('Document does not have the "{}"'.format(attr))

    def parse(self, file_path: str) -> list:
        documents = []
        with open(file_path, encoding=self.file_encoding) as file:
            logging.info('Parsing file: {}'.format(file_path))
            while True:
                line = file.readline()
                if not line:
                    logging.info('{} documents parsed from file: {}'.format(len(documents), file_path))
                    break

                if self.is_doc_start(line):
                    document = {}
                elif self.is_doc_no_start(line):
                    self._parse_doc_no(line, document)
                elif self.is_text_start(line):
                    self._parse_text(file, document)
                elif self.is_doc_end(line):
                    self._document_sanity_check(document)
                    documents.append(document)

        return documents

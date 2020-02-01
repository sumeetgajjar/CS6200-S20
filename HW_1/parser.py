import logging
import re


class TRECParser:
    def __init__(self, file_encoding='latin-1') -> None:
        self.file_encoding = file_encoding
        self.doc = {'start': '<DOC>', 'end': '</DOC>'}
        self.doc_no = {'start': '<DOCNO>', 'end': '</DOCNO>'}
        self.text = {'start': '<TEXT>', 'end': '</TEXT>'}
        self.head = {'start': '<HEAD>', 'end': '</HEAD>'}
        self.doc_no_regex = re.compile('<DOCNO>(.*)</DOCNO>')
        self.head_regex = re.compile('<HEAD>(.*)</HEAD>')
        self.head_start_regex = re.compile('<HEAD>(.*)')
        self.head_end_regex = re.compile('(.*)</HEAD>')

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

    def is_head_start(self, line: str) -> bool:
        return self._is_tag_start('head', line)

    def is_head_end(self, line: str) -> bool:
        return self._is_tag_end('head', line)

    def _parse_doc_no(self, line, document):
        if not self.is_doc_no_end(line):
            raise RuntimeError('line does not end with doc_no end tag')

        doc_no = self.doc_no_regex.match(line).group(1)
        doc_no = doc_no.strip()
        if not doc_no:
            raise RuntimeError('Empty doc_no in line: {}'.format(line))
        document['id'] = doc_no

    def _parse_text(self, file, document):
        lines = []
        while True:
            line = file.readline()
            if not line:
                raise RuntimeError('unexpected EOF while parsing text')

            if self.is_text_end(line):
                text = ''.join(lines)
                break

            lines.append(line)

        old_text = document.get('text', '')
        if old_text:
            text = '{}\n{}'.format(old_text, text)
        document['text'] = text

    def _parse_head(self, file, line, document):
        if self.is_head_end(line):
            head = self.head_regex.match(line).group(1)
        else:
            lines = [self.head_start_regex.match(line).group(1)]
            while True:
                line = file.readline()
                if not line:
                    raise RuntimeError('unexpected EOF while parsing head')

                if self.is_head_end(line):
                    lines.append(self.head_end_regex.match(line).group(1))
                    head = ''.join(lines)
                    break

                lines.append(line)

        old_head = document.get('head', '')
        if old_head:
            head = '{}\n{}'.format(old_head, head)
        document['head'] = head

    @staticmethod
    def _add_extra_info(document):
        document['length'] = document['text'].count(' ')

    @staticmethod
    def _document_sanity_check(document):
        for attr in ['id', 'text']:
            if attr not in document:
                raise RuntimeError('Document does not have the "{}"'.format(attr))

    def parse(self, file_path: str) -> list:
        documents = []
        with open(file_path, encoding=self.file_encoding) as file:
            logging.debug('Parsing file: {}'.format(file_path))
            while True:
                line = file.readline()
                if not line:
                    logging.debug('{} documents parsed from file: {}'.format(len(documents), file_path))
                    break

                if self.is_doc_start(line):
                    document = {}
                elif self.is_doc_no_start(line):
                    self._parse_doc_no(line, document)
                elif self.is_text_start(line):
                    self._parse_text(file, document)
                elif self.is_head_start(line):
                    self._parse_head(file, line, document)
                elif self.is_doc_end(line):
                    self._add_extra_info(document)
                    self._document_sanity_check(document)
                    documents.append(document)

        return documents

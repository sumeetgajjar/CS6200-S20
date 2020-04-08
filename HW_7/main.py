import email
import logging
import os
import random
import re
import string
from typing import Dict

import numpy as np
from bs4 import BeautifulSoup
from nltk import PorterStemmer, SnowballStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from utils.decorators import timing
from utils.utils import Utils


class Email:

    def __init__(self) -> None:
        self.subject = ''
        self.body = ''
        self.cleaned_subject_tokens = None
        self.cleaned_body_tokens = None
        self.file_name = None

    def __repr__(self) -> str:
        return "File: {file_name}, Subject: {subject}, Body: {body}" \
               "C_Subject:{cleaned_subject_tokens}, C_Body:{cleaned_body_tokens}".format(**self.__dict__)

    def __str__(self):
        return self.__repr__()


class HW7:
    _SPAM_EMAIL_DATA_DIR_PATH = '{}/SPAM_DATA/trec07p/data'.format(Utils.get_data_dir_abs_path(), )
    _SPAM_EMAIL_LABELS_PATH = '{}/SPAM_DATA/trec07p/full/index'.format(Utils.get_data_dir_abs_path())
    _SPLIT_REGEX = re.compile("\\s+")
    _PUNCTUATION_TABLE = str.maketrans('', '', string.punctuation)
    _STOPWORDS_SET = set(stopwords.words('english'))
    _STEMMER = SnowballStemmer('english')

    @classmethod
    @timing
    def _parse_labels(cls) -> Dict[str, int]:
        logging.info("Parsing labels")
        labels_dict = {}
        with open(cls._SPAM_EMAIL_LABELS_PATH, 'r') as file:
            for line in file:
                parts = re.split(cls._SPLIT_REGEX, line)
                if parts[0] == 'spam':
                    label = 1
                elif parts[0] == 'ham':
                    label = 0
                else:
                    raise ValueError("Invalid label")

                file_name = parts[1].split("/")[-1]
                labels_dict[file_name] = label

        logging.info("{} Labels parsed".format(len(labels_dict)))
        return labels_dict

    @classmethod
    def _clean_email(cls, raw_email: Email) -> Email:

        def _helper(text_to_clean):
            tokens = word_tokenize(text_to_clean)
            lowered_tokens = [token.lower() for token in tokens]
            stripped_tokens = [token.translate(cls._PUNCTUATION_TABLE) for token in lowered_tokens]
            word_tokens = [token for token in stripped_tokens if token.isalpha()]
            final_tokens = [token for token in word_tokens if token not in cls._STOPWORDS_SET]
            stemmed_tokens = [cls._STEMMER.stem(token) for token in final_tokens]

            return stemmed_tokens

        raw_email.cleaned_subject_tokens = _helper(raw_email.subject)
        raw_email.cleaned_body_tokens = _helper(raw_email.body)
        return raw_email

    @classmethod
    def _get_emails(cls) -> Email:
        email_files = os.listdir(cls._SPAM_EMAIL_DATA_DIR_PATH)
        logging.info("{} email files found".format(len(email_files)))
        for email_file in email_files:
            email_file_path = '{}/{}'.format(cls._SPAM_EMAIL_DATA_DIR_PATH, email_file)
            with open(email_file_path, 'r', encoding='ISO-8859-1') as email_file_fp:
                parsed_raw_email = cls._parse_raw_email(email_file_fp)
                parsed_raw_email.file_name = email_file

                cleaned_email = cls._clean_email(parsed_raw_email)

                yield cleaned_email

    @classmethod
    def _parse_email_payload_from_html(cls, raw_html) -> str:
        bs = BeautifulSoup(raw_html, 'html.parser')
        return bs.get_text().strip()

    @classmethod
    def _parse_raw_email(cls, email_file_fp) -> Email:

        def _helper(email_body):
            content_type = str(email_body.get_content_type())
            content_disposition = str(email_body.get_content_disposition())
            if content_type == 'text/plain' and 'attachment' not in content_disposition:
                parsed_email.body += str(email_body.get_payload())
            elif content_type == 'text/html' and 'attachment' not in content_disposition:
                parsed_email.body += cls._parse_email_payload_from_html(str(email_body.get_payload()))
            else:
                raise ValueError('Unknown content-type: {}, content-disposition:{}'.format(content_type,
                                                                                           content_disposition))

        body = email.message_from_file(email_file_fp)
        parsed_email = Email()

        if body['subject']:
            parsed_email.subject = body['subject']

        if body.is_multipart():
            for part in body.walk():
                _helper(part)
        else:
            _helper(body)

        return parsed_email

    @classmethod
    def main(cls):
        labels_dict = cls._parse_labels()
        print(next(cls._get_emails()))


if __name__ == '__main__':
    Utils.configure_logging()
    seed = 1234
    np.random.seed(seed)
    random.seed(seed)
    HW7.main()

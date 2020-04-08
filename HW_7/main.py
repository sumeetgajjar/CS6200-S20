import email
import logging
import os
import random
import re
from typing import Dict

import numpy as np
from bs4 import BeautifulSoup

from utils.decorators import timing
from utils.utils import Utils


class Email:

    def __init__(self) -> None:
        self.subject = ''
        self.body = ''
        self.file_name = None

    def __repr__(self) -> str:
        return "File: {file_name}, Subject: {subject}, Body: {body}".format(**self.__dict__)

    def __str__(self):
        return self.__repr__()


class HW7:
    _SPAM_EMAIL_DATA_DIR_PATH = '{}/SPAM_DATA/trec07p/data'.format(Utils.get_data_dir_abs_path(), )
    _SPAM_EMAIL_LABELS_PATH = '{}/SPAM_DATA/trec07p/full/index'.format(Utils.get_data_dir_abs_path())
    _SPLIT_REGEX = re.compile("\\s+")

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
    def _get_raw_emails(cls) -> Email:
        email_files = os.listdir(cls._SPAM_EMAIL_DATA_DIR_PATH)
        logging.info("{} email files found".format(len(email_files)))
        for email_file in email_files:
            email_file_path = '{}/{}'.format(cls._SPAM_EMAIL_DATA_DIR_PATH, email_file)
            with open(email_file_path, 'r', encoding='ISO-8859-1') as email_file_fp:
                parsed_email = cls._parse_raw_email(email_file_fp)
                parsed_email.file_name = email_file
                yield parsed_email

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
        print(next(cls._get_raw_emails()))


if __name__ == '__main__':
    Utils.configure_logging()
    seed = 1234
    np.random.seed(seed)
    random.seed(seed)
    HW7.main()

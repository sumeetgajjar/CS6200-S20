import email
import json
import logging
import os
import random
import re
import string
from typing import Dict

import numpy as np
from bs4 import BeautifulSoup
from nltk import SnowballStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from scipy.sparse import csr_matrix
from sklearn.datasets import dump_svmlight_file, load_svmlight_file
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import BernoulliNB
from sklearn.tree import DecisionTreeClassifier

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
    _CACHED_FEATURE_INDEX_NAME = 'feature_matrix_cache/feature_index.json'
    _CACHED_FEATURES_FILE_PATH = 'feature_matrix_cache/features.txt'
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
    def _text_cleaning_helper(cls, text_to_clean):
        cleaned_tokens = []
        tokens = word_tokenize(text_to_clean)
        for token in tokens:
            lowered_token = token.lower()
            stripped_token = lowered_token.translate(cls._PUNCTUATION_TABLE)
            if stripped_token.isalpha() and stripped_token not in cls._STOPWORDS_SET:
                cleaned_tokens.append(cls._STEMMER.stem(stripped_token))

        return cleaned_tokens

    @classmethod
    def _clean_email(cls, raw_email: Email) -> Email:
        raw_email.cleaned_subject_tokens = cls._text_cleaning_helper(raw_email.subject)
        raw_email.cleaned_body_tokens = cls._text_cleaning_helper(raw_email.body)
        return raw_email

    @classmethod
    def _get_emails(cls, email_files):
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
    def _get_email_contents_and_labels(cls, email_files, labels_dict, token_filter=lambda x: True):
        ix = 1
        email_contents = []
        labels = []
        for cleaned_email in cls._get_emails(email_files):
            ix += 1
            text = " ".join(filter(token_filter,
                                   cleaned_email.cleaned_subject_tokens + cleaned_email.cleaned_body_tokens))
            email_contents.append(text)

            file_name = cleaned_email.file_name
            labels.append(labels_dict[file_name])

            if ix % 1000 == 0:
                logging.info("Emails Read :{}".format(ix))

        return email_contents, labels

    @classmethod
    @timing
    def _generate_features(cls, use_cached=True):
        if use_cached:
            X, y = load_svmlight_file(cls._CACHED_FEATURES_FILE_PATH)
            with open(cls._CACHED_FEATURE_INDEX_NAME, 'r') as file:
                feature_name_index = json.load(file)
        else:
            labels_dict = cls._parse_labels()
            all_email_files = os.listdir(cls._SPAM_EMAIL_DATA_DIR_PATH)
            results = Utils.run_tasks_parallelly_in_chunks(cls._get_email_contents_and_labels, all_email_files, 12,
                                                           labels_dict=labels_dict)

            corpus = []
            all_labels = []
            for email_contents, labels in results:
                corpus.extend(email_contents)
                all_labels.extend(labels)

            vectorizer = CountVectorizer(ngram_range=(1, 1), min_df=0.02, max_df=0.95)
            X = vectorizer.fit_transform(corpus)
            #
            rows = list(range(len(all_labels)))
            cols = [0] * len(all_labels)
            y = csr_matrix((all_labels, (rows, cols)), shape=(len(rows), 1))

            feature_name_index = vectorizer.get_feature_names()
            dump_svmlight_file(X, y, f=cls._CACHED_FEATURES_FILE_PATH)
            with open(cls._CACHED_FEATURE_INDEX_NAME, 'w') as file:
                json.dump(feature_name_index, file)

        X_train, X_test, Y_train, Y_test = train_test_split(X, y, test_size=0.2)
        return X_train, X_test, Y_train, Y_test, feature_name_index

    @classmethod
    @timing
    def _run_model(cls, model, model_name, X_train, X_test, Y_train, Y_test, feature_name_index):
        logging.info("Running {}".format(model_name))
        model.fit(X_train, Y_train)

        def _run_prediction_phase(phase_name, X, Y_true):
            logging.info("Running {} predictions".format(phase_name))
            Y_predict = model.predict(X)
            Y_probs = model.predict_proba(X)[:, 1]
            auc_score = roc_auc_score(Y_true, Y_probs)
            logging.info("AUC score for {} for {} phase:{}".format(model_name, phase_name, auc_score))

        _run_prediction_phase('training', X_train, Y_train)
        _run_prediction_phase('testing', X_test, Y_test)

    @classmethod
    def main(cls):
        X_train, X_test, Y_train, Y_test, feature_name_index = cls._generate_features()
        for model, model_name in [
            (LogisticRegression(solver='newton-cg', fit_intercept=True), "LogisticRegression"),
            (DecisionTreeClassifier(max_depth=5), "DecisionTree-5"),
            (DecisionTreeClassifier(max_depth=10), "DecisionTree-10"),
            (DecisionTreeClassifier(max_depth=15), "DecisionTree-15"),
            (DecisionTreeClassifier(max_depth=20), "DecisionTree-20"),
            (BernoulliNB(), "BernoulliNB")
        ]:
            cls._run_model(model, model_name, X_train, X_test, Y_train, Y_test, feature_name_index)


if __name__ == '__main__':
    Utils.configure_logging()
    seed = 1234
    np.random.seed(seed)
    random.seed(seed)
    HW7.main()

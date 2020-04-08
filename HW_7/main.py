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
    _CACHED_FEATURE_INDEX_NAME_TEMPLATE = 'feature_matrix_cache/{}-{}-feature_index.json'
    _CACHED_FEATURES_FILE_PATH_TEMPLATE = 'feature_matrix_cache/{}-{}-features.txt'
    _CACHED_FILENAME_PATH_TEMPLATE = 'feature_matrix_cache/{}-{}-filename_index.txt'
    _SPLIT_REGEX = re.compile("\\s+")
    _PUNCTUATION_TABLE = str.maketrans('', '', string.punctuation)
    _STOPWORDS_SET = set(stopwords.words('english'))
    _STEMMER = SnowballStemmer('english')

    _PART_1_TRIAL_A_TOKENS_SET = None
    _PART_1_TRIAL_B_TOKENS_SET = None

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
    def _get_email_contents_and_labels(cls, email_files, labels_dict, token_filter):
        ix = 1
        email_contents = []
        labels = []
        for cleaned_email in cls._get_emails(email_files):
            ix += 1
            text = " ".join(filter(token_filter,
                                   cleaned_email.cleaned_subject_tokens + cleaned_email.cleaned_body_tokens))

            if text:
                email_contents.append(text)

                file_name = cleaned_email.file_name
                labels.append((labels_dict[file_name], file_name))

            if ix % 1000 == 0:
                logging.info("Emails Read :{}".format(ix))

        return email_contents, labels

    @classmethod
    @timing
    def _generate_features(cls, token_filter, use_cached=True, ngram_range=(1, 1)):
        feature_file_path = cls._CACHED_FEATURES_FILE_PATH_TEMPLATE.format(token_filter.__name__, ngram_range)
        feature_name_index_file_path = cls._CACHED_FEATURE_INDEX_NAME_TEMPLATE.format(token_filter.__name__,
                                                                                      ngram_range)
        filename_index_path = cls._CACHED_FILENAME_PATH_TEMPLATE.format(token_filter.__name__, ngram_range)

        if use_cached:
            X, y = load_svmlight_file(feature_file_path)
            with open(feature_name_index_file_path, 'r') as file, open(filename_index_path, 'r') as filename_index_file:
                feature_name_index = json.load(file)
                filename_index = json.load(filename_index_file)
        else:
            labels_dict = cls._parse_labels()
            all_email_files = os.listdir(cls._SPAM_EMAIL_DATA_DIR_PATH)
            results = Utils.run_tasks_parallelly_in_chunks(cls._get_email_contents_and_labels, all_email_files, 12,
                                                           # multi_process=False,
                                                           labels_dict=labels_dict,
                                                           token_filter=token_filter)

            corpus = []
            all_labels = []
            for email_contents, labels in results:
                corpus.extend(email_contents)
                all_labels.extend(labels)

            vectorizer = CountVectorizer(ngram_range=ngram_range, min_df=0.02, max_df=0.95)
            X = vectorizer.fit_transform(corpus)
            y = np.array([label[0] for label in all_labels])
            filename_index = [label[1] for label in all_labels]

            feature_name_index = vectorizer.get_feature_names()
            dump_svmlight_file(X, y, f=feature_file_path)
            with open(feature_name_index_file_path, 'w') as file, open(filename_index_path, 'w') as filename_index_file:
                json.dump(feature_name_index, file)
                json.dump(filename_index, filename_index_file)

        indices = np.arange(len(y))
        train_ix, test_ix = train_test_split(indices, test_size=0.2, shuffle=True)
        filename_index = np.array(filename_index)

        X_train, X_test, Y_train, Y_test, test_filename_index = \
            X[train_ix, :], X[test_ix, :], y[train_ix], y[test_ix], filename_index[test_ix]

        return X_train, X_test, Y_train, Y_test, feature_name_index, test_filename_index

    @classmethod
    def _run_model(cls, model, model_name, X_train, X_test, Y_train, Y_test, feature_name_index, test_filename_index):
        model.fit(X_train, Y_train)

        def _run_prediction_phase(phase_name, X, Y_true):
            Y_predict = model.predict(X)
            Y_probs = model.predict_proba(X)[:, 1]
            auc_score = roc_auc_score(Y_true, Y_probs)
            logging.info("AUC score for {} for {} phase:{}".format(model_name, phase_name, auc_score))
            return Y_probs

        # _run_prediction_phase('training', X_train, Y_train)
        scores = _run_prediction_phase('testing', X_test, Y_test)
        logging.info("Top 10 spam documents:{}".format(test_filename_index[np.argsort(scores)[::-1][:10]]))

    @classmethod
    def _part_1_trial_a_filter(cls, token):
        return token in cls._PART_1_TRIAL_A_TOKENS_SET

    @classmethod
    def _part_1_trial_b_filter(cls, token):
        return token in cls._PART_1_TRIAL_B_TOKENS_SET

    @classmethod
    def _part_2_token_filter(cls, token):
        return True

    @classmethod
    def main(cls):
        cls._PART_1_TRIAL_A_TOKENS_SET = cls._text_cleaning_helper(
            "free win porn click here hookups lottery trip tickets clearance meet singles biz credit fast cash off "
            "prize Congratulations urgent")

        cls._PART_1_TRIAL_B_TOKENS_SET = cls._text_cleaning_helper(
            "free spam click buy clearance shopper order earn cash extra money double collect credit check affordable "
            "fast price loans profit refinance hidden freedom chance miracle lose home remove success virus malware ad "
            "subscribe sales performance viagra valium medicine diagnostics million join deal unsolicited trial prize "
            "now legal bonus limited instant luxury legal celebrity only compare win viagra $$$ $discount click here "
            "meet singles incredible deal lose weight act now 100% free fast cash million dollars lower interest rate "
            "visit our website no credit check")

        for token_filter in [cls._part_1_trial_a_filter, cls._part_1_trial_b_filter, cls._part_2_token_filter]:
            logging.info("Using token filter:{}".format(token_filter.__name__))

            X_train, X_test, Y_train, Y_test, feature_name_index, test_filename_index = cls._generate_features(
                token_filter=token_filter,
                ngram_range=(1, 1))

            for model, model_name in [
                (LogisticRegression(solver='newton-cg', fit_intercept=True), "LogisticRegression"),
                (DecisionTreeClassifier(), "DecisionTree"),
                # (DecisionTreeClassifier(max_depth=5), "DecisionTree-5"),
                # (DecisionTreeClassifier(max_depth=10), "DecisionTree-10"),
                # (DecisionTreeClassifier(max_depth=15), "DecisionTree-15"),
                (BernoulliNB(), "BernoulliNB")
            ]:
                cls._run_model(model, model_name, X_train, X_test, Y_train, Y_test, feature_name_index,
                               test_filename_index)



if __name__ == '__main__':
    Utils.configure_logging()
    seed = 1234
    np.random.seed(seed)
    random.seed(seed)
    HW7.main()

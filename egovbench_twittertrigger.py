import logging
from logging.handlers import RotatingFileHandler

import time
import os

from egovbench_scorer import TwitterScorer
from egovbench_mongo import TwitterMongoConnector


class TwitterTrigger():

    def createdirectory(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def __init__(self):

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            logpath = '/home/addi/egovbench/logs/twitter/egovbench_twittertrigger.log'

            try:
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

                fh = RotatingFileHandler(logpath, maxBytes=20971520, backupCount=5)
                fh.setLevel(logging.DEBUG)
                fh.setFormatter(formatter)
                logger.addHandler(fh)

                ch = logging.StreamHandler()
                ch.setLevel(logging.INFO)
                ch.setFormatter(formatter)
                logger.addHandler(ch)

            except FileNotFoundError:
                self.createdirectory(logpath)

        self.tmc = TwitterMongoConnector()

    def prompt(self, texts):
        logging.info('[EGOVBENCH_TWITTERTRIGGER]>' + ' ' + texts)

    def launch(self):

        self.prompt('Launching trigger . . .')

        self.tmc.resetTemp()

        counter = 0

        while True:

            cursor = self.tmc.activateTailableCursor()

            while cursor.alive:
                try:
                    message = cursor.next()
                    self.prompt('(account_id: {}) Message received!'.format(message['id']))

                    self.pushAccountResult(message['id'])

                    self.prompt('===================================================================')

                    counter += 1

                    if counter % 100 == 0:
                        self.pushPostTypeResult()

                except StopIteration:
                    time.sleep(1)

    def pushPostTypeResult(self):

        ts = TwitterScorer(None)

        ts.getPostTypeStatisticDocument()

        ts.getPostTypeScoreDocument()

    def pushAccountResult(self, value):

        filter_dict = {'account_id': value}

        ts = TwitterScorer(filter_dict)

        accountStatisticDocument = ts.getAccountStatisticDocument()
        self.tmc.updateAccountResult(accountStatisticDocument)

        accountScoreDocument = ts.getAccountScoreDocument()
        self.tmc.updateAccountResult(accountScoreDocument)

        accountPostTypeScoreDocument = ts.getAccountPostTypeScoreDocument()
        self.tmc.updateAccountResult(accountPostTypeScoreDocument)

        self.tmc.updatePemdaScores(value)


if __name__ == '__main__':
    trigger = TwitterTrigger()
    trigger.pushPostTypeResult()
    trigger.launch()

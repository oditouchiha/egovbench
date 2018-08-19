import logging
from logging.handlers import RotatingFileHandler

import time
import os

from egovbench_scorer import FacebookScorer
from egovbench_mongo import FacebookMongoConnector


class FacebookTrigger():

    def createdirectory(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def __init__(self):

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            logpath = '/home/addi/egovbench/logs/facebook/egovbench_facebooktrigger.log'

            try:
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

        self.fmc = FacebookMongoConnector()

    def prompt(self, texts):
        logging.info('[EGOVBENCH_FACEBOOKTRIGGER]>' + ' ' + texts)

    def launch(self):

        self.prompt('Launching trigger . . .')

        self.fmc.resetTemp()

        counter = 0

        while True:

            cursor = self.fmc.activateTailableCursor()

            while cursor.alive:

                try:
                    message = cursor.next()
                    self.prompt('(page_id: {}) Message received!'.format(message['id']))

                    if message['page_type'] == 'resmi':

                        self.prompt('(page_id: {}) Resmi detected, calculating score . . .'.format(message['id']))

                        self.pushAccountResult(message['id'])

                        counter += 1

                    elif message['page_type'] == 'influencer':

                        self.prompt('(page_id: {}) Influencer detected, skipping . . .'.format(message['id']))

                    self.prompt('===================================================================')

                    if counter % 100 == 0:
                        self.pushPostTypeResult()

                except StopIteration:
                    time.sleep(1)

    def pushPostTypeResult(self):

        fs = FacebookScorer(None)

        fs.getPostTypeStatisticDocument()

        fs.getPostTypeScoreDocument()

    def pushAccountResult(self, value):

        filter_dict = {'page_id': value}

        fs = FacebookScorer(filter_dict)

        accountStatisticDocument = fs.getAccountStatisticDocument()
        self.fmc.updateAccountResult(accountStatisticDocument)

        accountScoreDocument = fs.getAccountScoreDocument()
        self.fmc.updateAccountResult(accountScoreDocument)

        accountPostTypeScoreDocument = fs.getAccountPostTypeScoreDocument()
        self.fmc.updateAccountResult(accountPostTypeScoreDocument)

        self.fmc.updatePemdaScores(value)


if __name__ == '__main__':
    trigger = FacebookTrigger()
    trigger.launch()

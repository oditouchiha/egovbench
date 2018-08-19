import logging
from logging.handlers import RotatingFileHandler

import time
import os

from egovbench_scorer import YoutubeScorer
from egovbench_mongo import YoutubeMongoConnector


class YoutubeTrigger():

    def createdirectory(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def __init__(self):

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            logpath = "/home/addi/egovbench/logs/youtube/egovbench_youtubetrigger.log"

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

        self.ymc = YoutubeMongoConnector()

    def prompt(self, texts):
        logging.info('[EGOVBENCH_YOUTUBETRIGGER]>' + ' ' + texts)

    def launch(self):

        self.prompt('Launching trigger . . .')

        self.ymc.resetTemp()

        while True:

            cursor = self.ymc.activateTailableCursor()

            while cursor.alive:

                try:
                    message = cursor.next()
                    self.prompt('(channel_id: {}) Message received!'.format(message['id']))

                    if message['channel_type'] == 'resmi':

                        self.prompt('(channel_id: {}) Resmi detected, calculating score . . .'.format(message['id']))

                        self.pushAccountResult(message['id'])

                    elif message['channel_type'] == 'influencer':

                        self.prompt('(channel_id: {}) Influencer detected, skipping . . .'.format(message['id']))

                    self.prompt('===================================================================')

                except StopIteration:
                    time.sleep(1)

    def pushAccountResult(self, value):

        filter_dict = {'channel_id': value}

        ys = YoutubeScorer(filter_dict)

        std = ys.getStatisticDocument()
        self.ymc.updateAccountResult(std)

        scd = ys.getScoreDocument()
        self.ymc.updateAccountResult(scd)

        self.ymc.updatePemdaScores(value)


if __name__ == '__main__':
    trigger = YoutubeTrigger()
    trigger.launch()

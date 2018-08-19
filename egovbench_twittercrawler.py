import tweepy
import logging
import time
import os
from logging.handlers import RotatingFileHandler
from configparser import SafeConfigParser
try:
    import simplejson as json
except ImportError:
    import json

from egovbench_parser import TwitterParser
from egovbench_mongo import TwitterMongoConnector
import egovbench_util as eu
from egovbench_exceptions import NoAccountException


class TwitterCrawler():

    def createdirectory(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def __init__(self, credFile='/home/addi/egovbench/apps/pythons/egovbench_credentials.ini', confFile='/home/addi/egovbench/apps/pythons/egovbench_config.ini'):

        confparser = SafeConfigParser()
        confparser.read(credFile)
        access_token = confparser.get('TwitterCredentials', 'access_token')
        access_token_secret = confparser.get('TwitterCredentials', 'access_token_secret')
        consumer_key = confparser.get('TwitterCredentials', 'consumer_key')
        consumer_secret = confparser.get('TwitterCredentials', 'consumer_secret')

        authHandler = tweepy.OAuthHandler(consumer_key, consumer_secret)
        authHandler.set_access_token(access_token, access_token_secret)
        self.twitterAPI = tweepy.API(authHandler, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        confparser2 = SafeConfigParser()
        confparser2.read(confFile)
        self.crawllimit = int(confparser2.get('CrawlerConfig', 'crawllimit'))

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            logpath = '/home/addi/egovbench/logs/twitter/egovbench_twittercrawler.log'

            try:
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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

        self.p = Pusher()
        self.tmc = TwitterMongoConnector()

    def prompt(self, texts):
        logging.info('[EGOVBENCH_TWITTERCRAWLER]>' + ' ' + texts)

    def launch(self):

        self.prompt('Launching . . .')

        accounts = self.tmc.collectAccounts()

        for account in accounts:

            pemda_id = account['_id']
            pemda_name = account['name']

            pemda_account = account['twitter_resmi']
            if pemda_account is not '':

                try:
                    self.crawlTweets(pemda_id, pemda_name, pemda_account)
                except NoAccountException as e:
                    logging.critical(e)
                    pass

    def crawlTweets(self, pemdaID, pemdaName, accountID):

        self.prompt('(pemda_id: {}, pemda_name: {}, pemda_account: {}) Crawl Started !'.format(pemdaID, pemdaName, accountID))

        # Mengecek apakah id akun tersebut ada di database

        account_exist = self.tmc.checkAccount(accountID.lower())

        # Bila ada, crawl akan di limit sebesar self.crawllimit. Bila tidak, akan mengcrawling akun secara menyeluruh.

        if account_exist:
            crawllimit = self.crawllimit
        else:
            crawllimit = None

        complete_list = []

        tweets_crawled = 0

        complete_dict = {}

        complete_dict['account'] = {}

        complete_dict['account']['account_id'] = accountID.lower()

        try:

            for tweets in tweepy.Cursor(
                    self.twitterAPI.user_timeline,
                    screen_name=accountID,
                    count=100,
                    include_rts=True,
                    tweet_mode='extended').items():

                json_str = json.dumps(tweets._json)
                j_results = json.loads(json_str)

                if 'RT @' not in j_results['full_text']:

                    # accout_id_number dan followerCount hanya dapat diambil setelah mendapatkan data hasil crawl (berupa tweet)

                    complete_dict['account']['account_id_number'] = j_results['user']['id_str']
                    complete_dict['account']['account_followerCount'] = j_results['user']['followers_count']

                    complete_dict['post'] = {}
                    complete_dict['post']['tweet_id'] = j_results['id_str']
                    complete_dict['post']['tweet_message'] = eu.cleanStrings(j_results['full_text'])
                    complete_dict['post']['tweet_createdDate'] = eu.formatTwitterTime(j_results['created_at'])
                    complete_dict['post']['tweet_retweetCount'] = j_results['retweet_count']
                    complete_dict['post']['tweet_favoriteCount'] = j_results['favorite_count']
                    complete_dict['post']['tweet_type'] = "text"
                    # Jenis tweet berupa text (dari hasil crawling) tidak memiliki atribut tweet_type, maka harus di-inisiasi secara manual

                    if 'entities' in j_results:
                        if 'media' in j_results['entities']:
                            complete_dict['post']['tweet_type'] = j_results['entities']['media'][0]['type']

                    if 'extended_entities' in j_results:
                        if 'media' in j_results['extended_entities']:
                            complete_dict['post']['tweet_type'] = j_results['extended_entities']['media'][0]['type']

                    complete_dict['post']['tweet_replyCount'] = 0

                    complete_list.append(complete_dict.copy())

                    # Counter

                    tweets_crawled += 1
                    self.prompt('(account_id: {}, tweet_id: {}) Tweets Crawled ! total: {}'.format(accountID, complete_dict['post']['tweet_id'], tweets_crawled))

                    # Berhenti crawling bila telah mencapai crawllimit

                    if tweets_crawled == crawllimit:
                        break

            if complete_list:

                # Mengambil angka reply dengan memanfaatkan API Search pada method collectReplies()

                self.collectReplies(complete_list)

                # Mem-push json/dict untuk membuat post document

                for one_complete_dict in complete_list:

                    self.p.pushPostDocument(one_complete_dict)

            # Mem-push json/dict untuk membuat account document

            self.p.pushAccountDocument(complete_dict)

        except tweepy.TweepError as e:
            logging.error(e)
            if e.reason == 'Twitter error response: status code = 404':
                raise NoAccountException

        self.prompt('(pemda_id: {}, pemda_name: {}, pemda_account: {}) Done Crawling !'.format(pemdaID, pemdaName, accountID))

    def collectReplies(self, completeList):

        '''
            Pengambilan reply dilakukan dengan mengcrawl akun dengan menggunakan API search dan
            menggunakan keyword (q) "@ + accountID".

            Selanjutnya, hasil crawl akan diproses dengan mencocokkan atribut ['in_reply_to_status_id_str']
            milik tweet hasil crawl dengan tweetID. Bila cocok, replies_collected akan bertambah 1.

            Terakhir, method ini akan mengembalikan nilai replies_collected sebagai jumlah reply dari tweetID.
        '''

        self.prompt("(account_id: {}) Collecting Replies . . .".format(completeList[0]['account']['account_id']))

        try:
            for tweets in tweepy.Cursor(
                    self.twitterAPI.search,
                    q='@' + completeList[0]['account']['account_id'],
                    include_rts=True,
                    tweet_mode='extended').items():

                json_str = json.dumps(tweets._json)
                j_results = json.loads(json_str)

                # Mencocokkan atribut

                for complete_dict in completeList:

                    if complete_dict['post']['tweet_id'] == j_results['in_reply_to_status_id_str']:

                        complete_dict['post']['tweet_replyCount'] += 1

                        self.prompt("(tweet_id: {}) {} Reply collected!".format(complete_dict['post']['tweet_id'], complete_dict['post']['tweet_replyCount']))

        except tweepy.TweepError as e:
            logging.error(e)
            if e.reason == 'Twitter error response: status code = 404':
                raise NoAccountException


class Pusher(TwitterCrawler):

    '''
        Kelas ini berfungsi untuk menggiring data keluar dari crawler, dan menyalurkannya ke fungsi
        dari masing-masing dokumen.
    '''

    def __init__(self):

        self.tp = TwitterParser()
        self.tmc = TwitterMongoConnector()

    def pushPostDocument(self, complete_dict):

        post_document = self.tp.getPostDocument(complete_dict)
        self.tmc.updatePost(post_document)

    def pushAccountDocument(self, complete_dict):

        for i in range(5, 0, -1):
            time.sleep(1)
            self.prompt('Updating account in {}'.format(i))

        account_document = self.tp.getAccountDocument(complete_dict)
        self.tmc.updateAccount(account_document)


if __name__ == '__main__':
    launcher = TwitterCrawler()
    launcher.launch()

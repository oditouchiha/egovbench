import tweepy
import logging
import time

from logging.handlers import RotatingFileHandler
from configparser import SafeConfigParser
try:
    import simplejson as json
except ImportError:
    import json

from egovbench_parser import TwitterParser
from egovbench_mongo import TwitterMongoConnector
from egovbench_gspreadsheet import TwitterCollector
import egovbench_util as eu
from egovbench_exceptions import NoAccountException


class TwitterCrawler():

    '''
        Kelas ini memiliki fungsi pokok untuk mengcrawl data twitter dengan memanfaatkan
        library tweepy.
    '''

    def __init__(self, credFile='egovbench_credentials.ini', confFile='egovbench_config.ini'):

        '''
        Inisiasi :

        - configparser

            Untuk mengambil credentials dari file egovbench_credentials.ini
            Untuk mengambil crawllimit dari file egovbench_config.ini

        - logger (Handler: Rotatingfilehandler, Streamhandler)

            Untuk sistem logging.

            Rotatingfilehandler berfungsi untuk menyimpan hasil
            logging ke dalam file dengan maksimal size sebesar maxBytes sebelum berpindah
            ke file selanjutnya. Jumlah maksimal file adalah sebesar backupCount.

            Sedangkan Streamhandler berfungsi untuk menampilkan hasil logging (setingkat info) ke dalam konsol
            sebagai alternatif dari print().
        '''

        confparser = SafeConfigParser()
        confparser.read(credFile)
        access_token = confparser.get('TwitterCredentials', 'access_token')
        access_token_secret = confparser.get('TwitterCredentials', 'access_token_secret')
        consumer_key = confparser.get('TwitterCredentials', 'consumer_key')
        consumer_secret = confparser.get('TwitterCredentials', 'consumer_secret')

        authHandler = tweepy.OAuthHandler(consumer_key, consumer_secret)
        authHandler.set_access_token(access_token, access_token_secret)
        self.twitterAPI = tweepy.API(authHandler, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        confparser2 = SafeConfigParser()
        confparser2.read(confFile)
        self.crawllimit = int(confparser2.get('CrawlerConfig', 'crawllimit'))

        if not logger.handlers:

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            fh = RotatingFileHandler('logs/twitter/egovbench_twittercrawler.log', maxBytes=20971520, backupCount=5)
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            logger.addHandler(ch)

        self.tc = TwitterCollector()
        self.p = Pusher()

    def prompt(self, texts):

        ''' Prompt dengan nama file untuk logging dengan level info agar mudah untuk di track'''

        logging.info('[EGOVBENCH_TWITTERCRAWLER]>' + ' ' + texts)

    def launch(self):

        '''
            Launching dengan mengambil data id, nama dan akun resmi pemda dari google spreadsheet.
            Mengandalkan file egovbench_spreadsheet untuk memperoleh konfigurasi dari class yang telah dibuat didalamnya.
        '''

        self.prompt('Launching . . .')

        pemda_id_list = self.tc.getPemdaIDList()
        pemda_name_list = self.tc.getPemdaNameList()
        pemda_account_list = self.tc.getPemdaAccountList()

        for pemda_id in pemda_id_list[1:]:

            pemda_name = pemda_name_list[pemda_id_list.index(pemda_id)]
            pemda_account = pemda_account_list[pemda_id_list.index(pemda_id)]

            if pemda_account is not '':

                try:
                    self.crawlTweets(pemda_id, pemda_name, pemda_account)
                except NoAccountException as e:
                    logging.critical(e)
                    pass

    def crawlTweets(self, pemdaID, pemdaName, accountID):

        '''
            Memulia crawling dengan argumen :
                pemdaID: ID pemda
                pemdaName: Nama pemda
                accountID: ID akun twitter resmi milik pemda

        '''

        self.prompt('(pemda_id: {}, pemda_name: {}, pemda_account: {}) Crawl Started !'.format(pemdaID, pemdaName, accountID))

        # Mengecek apakah id akun tersebut ada di database

        tmc = TwitterMongoConnector()
        account_exist = tmc.checkAccount(int(pemdaID), accountID.lower())

        # Bila ada, crawl akan di limit sebesar self.crawllimit. Bila tidak, akan mengcrawling akun secara menyeluruh.

        if account_exist:
            crawllimit = self.crawllimit
        else:
            crawllimit = None

        '''
            Memulai crawling:
            Hasil crawling akan diserialisasi ke dalam bentuk json/dict sebagai berikut:
            {
                pemda_id: <id pemda>
                pemda_name: <nama pemda>
                account: {
                    account_id: <id akun>
                    account_id_number: <id akun dalam bentuk nomor>
                    account_followerCount: <jumlah follower dari akun tersebut. Hanya dapat diambil dari data tweet (hasil crawl) yang masuk>
                }
                post: {
                    tweet_id: <id unik tweet>
                    tweet_message: <isi/pesan dari tweet>
                    tweet_createdDate: <tanggal dibuatnya tweet>
                    tweet_retweetCount: <jumlah retweet dari tweet>
                    tweet_favoriteCount: <jumlah favorite dari tweet>
                    tweet_type: <jenis tweet>
                    tweet_replyCount: <jumlah reply dari tweet. Dicari secara manual dengan menggunakan API search>
                }
            }
        '''

        try:
            tweets_crawled = 0

            complete_dict = {}

            # Mengubah pemdaID ke dalam bentuk int (bentuk string dari spreadsheet)

            complete_dict['pemda_id'] = int(pemdaID)
            complete_dict['pemda_name'] = pemdaName

            complete_dict['account'] = {}

            # Mengubah id akun dari spreadsheet ke huruf kecil

            complete_dict['account']['account_id'] = accountID.lower()

            complete_dict['post'] = {}

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

                    complete_dict['post']['tweet_id'] = j_results['id_str']
                    complete_dict['post']['tweet_message'] = eu.cleanStrings(j_results['full_text'])
                    complete_dict['post']['tweet_createdDate'] = eu.formatTwitterTime(j_results['created_at'])
                    complete_dict['post']['tweet_retweetCount'] = j_results['retweet_count']
                    complete_dict['post']['tweet_favoriteCount'] = j_results['favorite_count']

                    # Jenis tweet berupa text (dari hasil crawling) tidak memiliki atribut tweet_type, maka harus di-inisiasi secara manual

                    if 'media' in j_results['entities']:
                        complete_dict['post']['tweet_type'] = j_results['entities']['media'][0]['type']
                    else:
                        complete_dict['post']['tweet_type'] = "text"

                    # Counter

                    tweets_crawled += 1
                    self.prompt('(account_id: {}, tweet_id: {}) Tweets Crawled ! total: {}'.format(accountID, complete_dict['post']['tweet_id'], tweets_crawled))

                    # Mengambil angka reply dengan memanfaatkan API Search pada method collectReplies()

                    complete_dict['post']['tweet_replyCount'] = self.collectReplies(accountID, j_results['id_str'])

                    # Mem-push json/dict untuk membuat post document

                    self.p.pushPostDocument(complete_dict)

                    # Berhenti crawling bila telah mencapai crawllimit

                    if tweets_crawled == crawllimit:
                        break

            # Mem-push json/dict untuk membuat account document

            self.p.pushAccountDocument(complete_dict)

        except tweepy.TweepError as e:
            logging.error(e)
            if e.reason == 'Twitter error response: status code = 404':
                raise NoAccountException

        self.prompt('(pemda_id: {}, pemda_name: {}, pemda_account: {}) Done Crawling !'.format(pemdaID, pemdaName, accountID))

    def collectReplies(self, accountID, tweetID):

        '''
            Pengambilan reply dilakukan dengan mengcrawl akun dengan menggunakan API search dan
            menggunakan keyword (q) "@ + accountID".

            Selanjutnya, hasil crawl akan diproses dengan mencocokkan atribut ['in_reply_to_status_id_str']
            milik tweet hasil crawl dengan tweetID. Bila cocok, replies_collected akan bertambah 1.

            Terakhir, method ini akan mengembalikan nilai replies_collected sebagai jumlah reply dari tweetID.
        '''

        self.prompt("(tweet_id: {}) Collecting Replies . . .".format(tweetID))

        replies_collected = 0

        try:
            for tweets in tweepy.Cursor(
                    self.twitterAPI.search,
                    q='@' + accountID,
                    include_rts=True,
                    tweet_mode='extended').items():

                json_str = json.dumps(tweets._json)
                j_results = json.loads(json_str)

                # Mencocokkan atribut

                if j_results['in_reply_to_status_id_str'] == tweetID:

                    replies_collected += 1

        except tweepy.TweepError as e:
            logging.error(e)
            if e.reason == 'Twitter error response: status code = 404':
                raise NoAccountException

        self.prompt("(tweet_id: {}) Replies Collected ! total: {}".format(tweetID, replies_collected))

        return replies_collected


class Pusher():

    '''
        Kelas ini berfungsi untuk menggiring data keluar dari crawler, dan menyalurkannya ke fungsi
        dari masing-masing dokumen.
    '''

    def __init__(self):

        '''
            Inisiasi:

            -TwitterParser():

                Diambil dari file egovbench_parser.py, kelas ini berfungsi untuk memparsing json/dict hasil
                crawling ke dalam struktur penyimpanan document di MongoDB

            -TwitterMongoConnector():

                Diambil dari file egovbench_mongo.py, kelas ini berfungsi untuk menyalurkan hasil parsing
                ke dalam MongoDB.
        '''

        self.tp = TwitterParser()
        self.tmc = TwitterMongoConnector()

    def pushPostDocument(self, complete_dict):

        '''
            Fungsi ini memanggil fungsi getPostDocument() dari kelas TwitterParser() untuk mengubah data
            crawling ke dalam struktur post document, dan memasukannya ke dalam post collection di MongoDB
            dengan menggunakan fungsi updatePost() milik TwitterMongoConnector()
        '''

        post_document = self.tp.getPostDocument(complete_dict)
        self.tmc.updatePost(post_document)

    def pushAccountDocument(self, complete_dict):

        '''
            Fungsi ini memanggil fungsi getAccountDocument() dari kelas TwitterParser() untuk mengubah data
            crawling ke dalam struktur account document, dan memasukannya ke dalam account collection di MongoDB
            dengan menggunakan fungsi updateAccount() milik TwitterMongoConnector()
        '''

        account_document = self.tp.getAccountDocument(complete_dict)
        self.tmc.updateAccount(account_document)


if __name__ == '__main__':
    launcher = TwitterCrawler()
    launcher.launch()

import datetime
import time
import logging
from logging.handlers import RotatingFileHandler
from configparser import SafeConfigParser

try:
    import simplejson as json
except ImportError:
    import json

try:
    from urllib.request import urlopen, Request, HTTPError
except ImportError:
    from urllib2 import urlopen, Request
import requests

from egovbench_gspreadsheet import FacebookCollector
from egovbench_mongo import FacebookMongoConnector
from egovbench_parser import FacebookParser
# from egovbench_kafka import FacebookKafkaPost, FacebookKafkaComment
import egovbench_util as eu
from egovbench_exceptions import NoAccountException


class FacebookCrawler():

    def __init__(self, credFile='egovbench_credentials.ini', confFile='egovbench_config.ini'):

        credparser = SafeConfigParser()
        credparser.read(credFile)
        self.access_token = credparser.get('FacebookCredentials', 'access_token')

        confparser2 = SafeConfigParser()
        confparser2.read(confFile)
        self.crawllimit = int(confparser2.get('CrawlerConfig', 'crawllimit'))

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            fh = RotatingFileHandler('logs/facebook/egovbench_facebook.log', maxBytes=20971520, backupCount=5)
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            logger.addHandler(ch)

        self.p = Pusher()
        self.fc = FacebookCollector()
        self.fmc = FacebookMongoConnector()

    def prompt(self, texts):
        return logging.info('[EGOVBENCH_FACEBOOKCRAWLER]>' + ' ' + texts)

    def launch(self):
        self.prompt('Launching . . .')

        accounts = self.fmc.collectAccounts()

        for account in accounts[98:]:

            pemda_id = account['_id']
            pemda_name = account['name']

            resmi_page_id = account['facebook_resmi']
            if resmi_page_id is not '':
                try:
                    self.crawlPost(pemda_id, pemda_name, 'resmi', resmi_page_id)
                except NoAccountException as e:
                    logging.critical(e)
                    pass

            # influencer_page_id = account['facebook_influencer']
            # if influencer_page_id is not '':
            #     try:
            #         self.crawlPost(pemda_id, pemda_name, 'influencer', influencer_page_id)
            #     except NoAccountException as e:
            #         logging.critical(e)
            #         pass

    def crawlPost(self, pemdaID, pemdaName, pageType, pageID):
        self.prompt('(pemda_id: {}, pemda_name: {}, page_type: {}, page_id: {}) Crawl Started!'.format(
            pemdaID,
            pemdaName,
            pageType,
            pageID
        ))

        channel_exist = self.fmc.checkAccount(pageID.lower())

        if channel_exist:
            crawllimit = self.crawllimit
        else:
            crawllimit = None

        complete_dict = {}

        complete_dict['account'] = {}
        complete_dict['account']['page_id'] = pageID.lower()
        complete_dict['account']['page_type'] = pageType

        base = "https://graph.facebook.com/v3.0"
        node = "/{}".format(pageID)
        parameters = "?access_token={}&fields=id,name,fan_count,posts.limit({})".format(self.access_token, self.crawllimit)
        since_date = "2016-01-01"
        until_date = ""

        since = ".since({})".format(since_date) if since_date \
            is not '' else ''
        until = ".until({})".format(until_date) if until_date \
            is not '' else ''

        after = ''

        fields = "{message,link,created_time,type,name,id,comments.limit(0).summary(true),shares," + \
                 "reactions.type(LIKE).summary(total_count).limit(0).as(like)," +\
                 "reactions.type(LOVE).summary(total_count).limit(0).as(love)," +\
                 "reactions.type(WOW).summary(total_count).limit(0).as(wow)," +\
                 "reactions.type(HAHA).summary(total_count).limit(0).as(haha)," +\
                 "reactions.type(SAD).summary(total_count).limit(0).as(sad)," +\
                 "reactions.type(ANGRY).summary(total_count).limit(0).as(angry)}"

        posts_crawled = 0

        searchnextpage = True
        while searchnextpage:

            url = base + node + parameters + since + until + after + fields
            j_input = json.loads(self.requestUntilSucceed(url).decode())

            complete_dict['account']['page_id_number'] = j_input['id']
            complete_dict['account']['page_name'] = j_input['name']
            complete_dict['account']['page_fanCount'] = j_input['fan_count']

            if 'posts' in j_input:

                for post in j_input['posts']['data']:

                    complete_dict['post'] = {}
                    complete_dict['post']['post_id'] = post['id']
                    complete_dict['post']['post_type'] = post['type']
                    complete_dict['post']['post_message'] = '' if 'message' not in post else eu.cleanStrings(post['message'])
                    complete_dict['post']['post_createdtime'] = eu.formatFacebookTime(post['created_time'])

                    complete_dict['post']['post_commentCount'] = 0 if 'comments' not in post else post['comments']['summary']['total_count']
                    complete_dict['post']['post_shareCount'] = 0 if 'shares' not in post else post['shares']['count']

                    complete_dict['post']['post_reaction'] = {}
                    complete_dict['post']['post_reaction']['like'] = post['like']['summary']['total_count']
                    complete_dict['post']['post_reaction']['love'] = post['love']['summary']['total_count']
                    complete_dict['post']['post_reaction']['wow'] = post['wow']['summary']['total_count']
                    complete_dict['post']['post_reaction']['haha'] = post['haha']['summary']['total_count']
                    complete_dict['post']['post_reaction']['sad'] = post['sad']['summary']['total_count']
                    complete_dict['post']['post_reaction']['angry'] = post['angry']['summary']['total_count']

                    self.p.pushPostDocument(complete_dict)

                    # returned_comments = []
                    # returned_comments = self.collectPostComments(post['id'])

                    # complete_dict['comment'] = returned_comments

                    # self.p.pushCommentDocument(complete_dict)

                    posts_crawled += 1

                    self.prompt('(page_id: {}) {} Post crawled!'.format(
                            pageID,
                            posts_crawled
                    ))

                    if posts_crawled == crawllimit:
                        searchnextpage = False
                        break

                after = ".after({})".format(j_input['posts']['paging']['cursors']['after'])

            else:
                searchnextpage = False

            self.prompt("(page_id: {}) All Post crawled! total: {}".format(pageID, posts_crawled))

        self.p.pushAccountDocument(complete_dict)

        self.prompt('(pemda_id: {}, pemda_name: {}, page_type: {}, page_id: {}) Finished crawling!'.format(
            pemdaID,
            pemdaName,
            pageType,
            pageID
        ))

    def collectPostComments(self, postID):
        self.prompt("(post_id: {}) Collecting post's comments . . .".format(postID))

        comment_list_returned = []

        comments_collected = 0

        base = "https://graph.facebook.com/v3.0"
        node = "/{}".format(postID)
        parameters = "?access_token={}&fields=comments".format(self.access_token)
        nextcommentpage = ''

        url = base + node + parameters + nextcommentpage

        searchnextpage = True
        while searchnextpage:

            data = json.loads(self.requestUntilSucceed(url).decode())

            if 'comments' in data:

                for comment in data['comments']['data']:

                    comment_dict = {}
                    comment_dict['comment_id'] = comment['id']
                    comment_dict['comment_message'] = eu.cleanStrings(comment['message'])
                    comment_dict['comment_createdDate'] = eu.formatFacebookTime(comment['created_time'])

                    comment_list_returned.append(comment_dict)

                    comments_collected += 1

                    if comments_collected % 10 == 0:
                        self.prompt("(post_id: {}) {} Comments collected!".format(postID, comments_collected))

                if 'next' in data['comments']['paging']:
                    url = data['comments']['paging']['next']
                else:
                    searchnextpage = False

            else:
                searchnextpage = False

        self.prompt("(post_id: {}) All Post's comments collected! total: {}".format(postID, comments_collected))

        return comment_list_returned

    def requestUntilSucceed(self, url):

        for i in range(5, 0, -1):
            time.sleep(1)
            self.prompt('Sleeping to avoid rate limit. Continuing in {}'.format(i))

        req = Request(url)
        success = False
        while success is False:
            try:
                response = urlopen(req)
                if response.getcode() == 200:
                    success = True

            except HTTPError as err:
                if err.code == 404:
                    raise NoAccountException

            except requests.exceptions.SSLError as e:
                logging.error('ERROR: ' + e)

            except Exception as e:
                logging.error(e)
                time.sleep(1)

                logging.error("Error for URL {}: {}".format(url, datetime.datetime.now()))
                logging.error("Retrying...")

        return response.read()


class Pusher(FacebookCrawler):

    def __init__(self):
        self.fp = FacebookParser()
        self.fmc = FacebookMongoConnector()
        # self.fkp = FacebookKafkaPost()
        # self.fkc = FacebookKafkaComment()

    def pushPostDocument(self, complete_dict):

        '''
            Fungsi ini memanggil fungsi getPostDocument() dari kelas TwitterParser() untuk mengubah data
            crawling ke dalam struktur post document, dan memasukannya ke dalam post collection di MongoDB
            dengan menggunakan fungsi updatePost() milik TwitterMongoConnector()
        '''
        post_document = self.fp.getPostDocument(complete_dict)
        self.fmc.updatePost(post_document)

        # for i in range(2, 0, -1):
        #     time.sleep(1)
        #     self.prompt('Sending message to stream in {}'.format(i))

        # self.fkp.send_message(post_document)

    def pushCommentDocument(self, complete_dict):

        comment_document = self.fp.getCommentDocument(complete_dict)
        self.fmc.updateComment(comment_document)

            # for i in range(2, 0, -1):
            #     time.sleep(1)
            #     self.prompt('Sending message to stream in {}'.format(i))

            # self.fkc.send_message(comment)

    def pushAccountDocument(self, complete_dict):

        '''
            Fungsi ini memanggil fungsi getAccountDocument() dari kelas TwitterParser() untuk mengubah data
            crawling ke dalam struktur account document, dan memasukannya ke dalam account collection di MongoDB
            dengan menggunakan fungsi updateAccount() milik TwitterMongoConnector()
        '''

        for i in range(20, 0, -1):
            time.sleep(1)
            self.prompt('Updating account in {}'.format(i))

        account_document = self.fp.getAccountDocument(complete_dict)
        self.fmc.updateAccount(account_document)


if __name__ == '__main__':
    launcher = FacebookCrawler()
    launcher.launch()

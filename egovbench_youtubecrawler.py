from __future__ import division

try:
    import simplejson as json
except ImportError:
    import json

import requests
import warnings
import logging
import os
from logging.handlers import RotatingFileHandler
from configparser import SafeConfigParser

from egovbench_mongo import YoutubeMongoConnector
from egovbench_parser import YoutubeParser
from egovbench_kafka import YoutubeKafkaPost, YoutubeKafkaComment
import egovbench_util as eu
from egovbench_exceptions import NoAccountException

warnings.filterwarnings('ignore')


class IDChecker():

    def __init__(self, api_key):
        self.api_key = api_key

    def prompt(self, texts):
        return logging.info('[EGOVBENCH_YOUTUBECRAWLER_IDCHECKER]>' + ' ' + texts)

    def checkID(self, channelID):
        self.prompt('(channel_id: {}) Checking if input is ID . . .'.format(channelID))
        parameters = {
            "part": "snippet",
            "id": channelID,
            "key": self.api_key,
            "fields": "items/snippet/title"
        }
        url = "https://www.googleapis.com/youtube/v3/channels"
        page = requests.request(method='get', url=url, params=parameters)
        data = self.retryUntilSuccess(page)
        j_results = json.loads(data)

        idconverter = {}

        if j_results['items']:
            idconverter['title'] = j_results['items'][0]['snippet']['title']
            idconverter['id'] = channelID
        else:
            idconverter = self.convertUsernametoID(channelID)

        self.prompt('(channel_id: {}, channel_name: {}) Done Checking !'.format(idconverter['id'], idconverter['title']))

        return idconverter

    def convertUsernametoID(self, channelID):
        self.prompt('(channel_id: {}) Username input detected!'.format(channelID))

        parameters = {
            "part": "snippet",
            "forUsername": channelID,
            "key": self.api_key,
            "fields": "items(id,snippet/title)"
        }
        url = "https://www.googleapis.com/youtube/v3/channels"
        page = requests.request(method='get', url=url, params=parameters)
        data = self.retryUntilSuccess(page)
        j_results = json.loads(data)

        usernameconverter = {}

        if j_results['items']:
            usernameconverter['title'] = j_results['items'][0]['snippet']['title']
            usernameconverter['id'] = j_results['items'][0]['id']
        else:
            raise NoAccountException

        self.prompt('(channel_id: {}) Username Converted!'.format(usernameconverter['id']))

        return usernameconverter

    def retryUntilSuccess(self, page):

        try:
            data = page.text
            if page.status_code != 200:
                logging.error('ERROR:' + str(page.status_code) + ' ' + page.url)

        except requests.exceptions.SSLError as e:
            logging.error('ERROR: ' + e)

        except requests.exceptions.HTTPError as e:
            logging.error('ERROR: ' + e)

        return data


class YoutubeCrawler():

    def createdirectory(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def __init__(self, credFile='/home/addi/egovbench/apps/pythons/egovbench_credentials.ini', confFile='/home/addi/egovbench/apps/pythons/egovbench_config.ini'):

        credparser = SafeConfigParser()
        credparser.read(credFile)
        self.api_key = credparser.get('YoutubeCredentials', 'api_key')

        confparser2 = SafeConfigParser()
        confparser2.read(confFile)
        self.crawllimit = int(confparser2.get('CrawlerConfig', 'crawllimit'))

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            logpath = '/home/addi/egovbench/logs/youtube/egovbench_youtubecrawler.log'

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
        self.ymc = YoutubeMongoConnector()

    def prompt(self, texts):
        return logging.info('[EGOVBENCH_YOUTUBECRAWLER]>' + ' ' + texts)

    def launch(self):
        self.prompt('Launching . . .')

        accounts = self.ymc.collectAccounts()

        for account in accounts:

            pemda_id = account['_id']
            pemda_name = account['name']

            resmi_channel_id = account['youtube_resmi']
            if resmi_channel_id is not '':
                try:
                    self.crawlVideo(pemda_id, pemda_name, 'resmi', resmi_channel_id)
                except NoAccountException as e:
                    logging.critical(e)
                    pass

            influencer_channel_id = account['youtube_influencer']
            if influencer_channel_id is not '':
                try:
                    self.crawlVideo(pemda_id, pemda_name, 'influencer', influencer_channel_id)
                except NoAccountException as e:
                    logging.critical(e)
                    pass

    def crawlVideo(self, pemdaID, pemdaName, channelType, channelID):
        self.prompt('(pemda_id: {}, pemda_name: {}, channel_type: {}, channel_id: {}) Crawl Started!'.format(
            pemdaID,
            pemdaName,
            channelType,
            channelID
        ))

        channel_exist = self.ymc.checkAccount(channelID.lower())

        if channel_exist:
            crawllimit = self.crawllimit
        else:
            crawllimit = None

        checker = IDChecker(self.api_key)
        checked = checker.checkID(channelID)
        channel_name = checked['title']
        channel_id = checked['id']

        complete_dict = {}

        complete_dict['account'] = {}
        complete_dict['account']['channel_id'] = channelID.lower()
        complete_dict['account']['channel_name'] = channel_name
        complete_dict['account']['channel_type'] = channelType

        returned_channel_statistics = {}
        returned_channel_statistics = self.collectChannelStatistics(channel_id)

        complete_dict['account']['channel_subscriberCount'] = int(returned_channel_statistics['channel_subscriberCount'])

        parameters = {
            "part": "snippet",
            "channelId": channel_id,
            "maxResults": crawllimit,
            "key": self.api_key,
            "type": "video",
            "fields": "items(id/videoId,snippet/channelTitle),nextPageToken"
        }
        url = "https://www.googleapis.com/youtube/v3/search"

        videos_crawled = 0

        searchnextpage = True

        while searchnextpage:

            page = requests.request(method="get", url=url, params=parameters)
            data = self.retryUntilSuccess(page)
            j_results = json.loads(data)

            if j_results['items']:

                for video in j_results['items']:

                        complete_dict['post'] = {}
                        complete_dict['post']['video_id'] = video['id']['videoId']

                        returned_video_statistics = {}
                        returned_video_statistics = self.collectVideoStatistics(video['id']['videoId'])

                        complete_dict['post']['video_title'] = returned_video_statistics['video_title']
                        complete_dict['post']['video_createdDate'] = returned_video_statistics['video_createdDate']

                        complete_dict['post']['video_dislikeCount'] = int(returned_video_statistics['dislikeCount']) if 'dislikeCount' in returned_video_statistics else 0
                        complete_dict['post']['video_likeCount'] = int(returned_video_statistics['likeCount']) if 'likeCount' in returned_video_statistics else 0
                        complete_dict['post']['video_viewCount'] = int(returned_video_statistics['viewCount']) if 'viewCount' in returned_video_statistics else 0
                        complete_dict['post']['video_commentCount'] = int(returned_video_statistics['commentCount']) if 'commentCount' in returned_video_statistics else 0

                        self.p.pushPostDocument(complete_dict)

                        returned_comments = {}
                        returned_comments = self.collectVideoComments(video['id']['videoId'])

                        complete_dict['comment'] = returned_comments

                        self.p.pushCommentDocument(complete_dict)

                        videos_crawled += 1

                        self.prompt('(channel_id: {}) {} Videos crawled!'.format(
                            channelID,
                            videos_crawled
                        ))

                        if videos_crawled == crawllimit:
                            searchnextpage = False
                            break

                if 'nextPageToken' in j_results:
                    parameters['pageToken'] = j_results['nextPageToken']

            else:
                searchnextpage = False

            self.prompt("(channel_id: {}) All videos crawled! total: {}".format(channelID, videos_crawled))

        self.p.pushAccountDocument(complete_dict)

        self.prompt('(pemda_id: {}, pemda_name: {}, channel_type: {}, channel_id: {}) Finished Crawling!'.format(
            pemdaID,
            pemdaName,
            channelType,
            channelID
        ))

    def collectChannelStatistics(self, channelID):
        self.prompt("(channel_id: {}) Collecting channel's statistics . . .".format(channelID))

        stat_dict_returned = {}
        parameters = {
            "part": "statistics",
            "id": channelID,
            "key": self.api_key,
            "fields": "items(statistics(subscriberCount))"
        }
        url = "https://www.googleapis.com/youtube/v3/channels"

        page = requests.request(method="get", url=url, params=parameters)
        data = self.retryUntilSuccess(page)
        stat_results = json.loads(data)

        stat_dict_returned['channel_subscriberCount'] = stat_results['items'][0]['statistics']['subscriberCount']

        self.prompt("(channel_id: {}) Channel's statistics collected!".format(channelID))

        return stat_dict_returned

    def collectVideoStatistics(self, videoID):
        self.prompt("(video_id: {}) Collecting video's statistics . . .".format(videoID))

        stat_dict_returned = {}

        parameters = {
            "part": "statistics,snippet",
            "id": videoID,
            "key": self.api_key,
            "fields": "items(id,snippet(publishedAt,title),statistics(commentCount,dislikeCount,likeCount,viewCount))"
        }
        url = "https://www.googleapis.com/youtube/v3/videos"

        page = requests.request(method="get", url=url, params=parameters)
        data = self.retryUntilSuccess(page)
        stat_results = json.loads(data)

        stat_dict_returned['video_statistic'] = stat_results['items'][0]['statistics']

        video_title = stat_results['items'][0]['snippet']['title']
        stat_dict_returned['video_title'] = eu.cleanStrings(video_title)

        video_createdDate = stat_results['items'][0]['snippet']['publishedAt']
        stat_dict_returned['video_createdDate'] = eu.formatYoutubeTime(video_createdDate)

        self.prompt("(video_id: {}) Video's statistics collected!".format(videoID))

        return stat_dict_returned

    def collectVideoComments(self, videoID):
        self.prompt("(video_id: {}) Collecting video's comments . . .".format(videoID))

        comm_dict_returned = []
        parameters = {
            "part": "snippet",
            "maxResults": 100,
            "videoId": videoID,
            "key": self.api_key,
            "fields": "items(snippet(topLevelComment(id,snippet(publishedAt,textOriginal)))),nextPageToken"
        }
        url = "https://www.googleapis.com/youtube/v3/commentThreads"
        nextPageToken = ''
        has_next_page = True

        comments_collected = 0

        while has_next_page:
            parameters['pageToken'] = '' if '' else nextPageToken

            page = requests.request(method="get", url=url, params=parameters)
            data = self.retryUntilSuccess(page)
            comment_results = json.loads(data)

            if 'items' in comment_results and comment_results['items']:

                for comment in comment_results['items']:
                    comm_dict = {}
                    comm_dict['comment_id'] = comment['snippet']['topLevelComment']['id']

                    comment_message = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                    comm_dict['comment_message'] = eu.cleanStrings(comment_message)

                    comment_createdDate = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    comm_dict['comment_createdDate'] = eu.formatYoutubeTime(comment_createdDate)

                    comm_dict_returned.append(comm_dict)

                    comments_collected += 1

                    if comments_collected % 10 == 0:
                        self.prompt("(video_id: {}) {} Comments collected!".format(videoID, comments_collected))

            if 'nextPageToken' in comment_results:
                nextPageToken = comment_results['nextPageToken']
            else:
                has_next_page = False

        self.prompt("(video_id: {}) All Video's comments collected! total: {}".format(videoID, comments_collected))

        return comm_dict_returned

    def retryUntilSuccess(self, page):

        try:
            data = page.text
            if page.status_code != 200:
                logging.error('ERROR:' + str(page.status_code) + ' ' + page.url)

        except requests.exceptions.SSLError as e:
            logging.error('ERROR: ' + e)

        except requests.exceptions.HTTPError as e:
            logging.error('ERROR: ' + e)

        return data


class Pusher(YoutubeCrawler):

    def __init__(self):
        self.yp = YoutubeParser()
        self.ymc = YoutubeMongoConnector()
        self.ykp = YoutubeKafkaPost()
        self.ykc = YoutubeKafkaComment()

    def pushPostDocument(self, complete_dict):

        post_document = self.yp.getPostDocument(complete_dict)
        self.ymc.updatePost(post_document)
        self.ykp.send_message(post_document)

    def pushCommentDocument(self, complete_dict):

        comment_document = self.yp.getCommentDocument(complete_dict)

        for comment in comment_document:
            self.ymc.updateComment(comment)
            self.ykc.send_message(comment)

    def pushAccountDocument(self, complete_dict):

        account_document = self.yp.getAccountDocument(complete_dict)
        self.ymc.updateAccount(account_document)


if __name__ == '__main__':
    launcher = YoutubeCrawler()
    launcher.launch()

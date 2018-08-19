import time
import pymongo
import logging


class TwitterScorer():

    def __init__(self, accountID):
        client = pymongo.MongoClient()
        db = client['experimental']
        self.account_collection = db['twitter_accounts']
        self.post_collection = db['twitter_posts']

        self.account_id = accountID

    def getFieldNotZeroCount(self, field):
        account_fieldnotzeroCount = self.post_collection.count({'account_id': self.account_id, field: {'$ne': 0}})
        return account_fieldnotzeroCount

    def getFieldCount(self, field):
        account_fieldCount = self.post_collection.aggregate(
            [
                {
                    '$match': {
                        'account_id': self.account_id
                    }
                },
                {
                    '$group': {
                        '_id': '$account_id',
                        'total': {
                            '$sum': '$%s' % (field)
                        }
                    }
                }
            ]
        )
        account_fieldCount = list(account_fieldCount)[0]['total']
        logging.info(account_fieldCount)
        return account_fieldCount

    def getTweetCount(self):
        account_tweetCount = self.post_collection.count({'account_id': self.account_id})
        return account_tweetCount

    def getFollowerCount(self):
        account_followerCount = self.account_collection.find({'account_id': self.account_id}, {'account_followerCount': 1})
        account_followerCount = account_followerCount[0]['account_followerCount']
        return account_followerCount

    def createSP1(self, field):
        field_notzeroCount = self.getFieldNotZeroCount(field)
        account_tweetCount = self.getTweetCount()
        sub_parameter_1 = field_notzeroCount / account_tweetCount

        return sub_parameter_1

    def createSP2(self, field):
        account_fieldCount = self.getFieldCount(field)
        account_tweetCount = self.getTweetCount()
        sub_parameter_2 = account_fieldCount / account_tweetCount

        return sub_parameter_2

    def createSP3(self, field):
        sub_parameter_2 = self.createSP2(field)
        account_followerCount = self.getFollowerCount()
        sub_parameter_3 = sub_parameter_2 / account_followerCount

        return sub_parameter_3

    def getP1(self):
        return self.createSP1('tweet_favoriteCount')

    def getP2(self):
        return self.createSP2('tweet_favoriteCount')

    def getP3(self):
        return self.createSP3('tweet_favoriteCount')

    def getC1(self):
        return self.createSP1('tweet_replyCount')

    def getC2(self):
        return self.createSP2('tweet_replyCount')

    def getC3(self):
        return self.createSP3('tweet_replyCount')

    def getV1(self):
        return self.createSP1('tweet_retweetCount')

    def getV2(self):
        return self.createSP2('tweet_retweetCount')

    def getV3(self):
        return self.createSP3('tweet_retweetCount')

    def e(self):
        return self.getP3() + self.getC3() + self.getV3()

    def getUpdateDocument(self):

        update_document = {}
        update_document['result'] = {}

        update_document['result']['tweet_statistics'] = {}
        update_document['result']['tweet_statistics']['account_tweetCount'] = self.getTweetCount()
        update_document['result']['tweet_statistics']['account_favoriteCount'] = self.getFieldCount('tweet_favoriteCount')
        update_document['result']['tweet_statistics']['account_replyCount'] = self.getFieldCount('tweet_replyCount')
        update_document['result']['tweet_statistics']['account_retweetCount'] = self.getFieldCount('tweet_retweetCount')

        update_document['result']['tweet_score'] = {}

        update_document['result']['tweet_score']['popularity'] = {}
        update_document['result']['tweet_score']['popularity']['popularity_1'] = self.getP1()
        update_document['result']['tweet_score']['popularity']['popularity_2'] = self.getP2()
        update_document['result']['tweet_score']['popularity']['popularity_3'] = self.getP3()

        update_document['result']['tweet_score']['commitment'] = {}
        update_document['result']['tweet_score']['commitment']['commitment_1'] = self.getP1()
        update_document['result']['tweet_score']['commitment']['commitment_2'] = self.getP2()
        update_document['result']['tweet_score']['commitment']['commitment_3'] = self.getP3()

        update_document['result']['tweet_score']['virality'] = {}
        update_document['result']['tweet_score']['virality']['virality_1'] = self.getP1()
        update_document['result']['tweet_score']['virality']['virality_2'] = self.getP2()
        update_document['result']['tweet_score']['virality']['virality_3'] = self.getP3()

        update_document['result']['tweet_score']['engagementindex'] = self.e()

        return update_document


class Trigger():

    def __init__(self):

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            logger.addHandler(ch)

        client = pymongo.MongoClient()
        db = client['experimental']
        db.drop_collection('twitter_temp')
        db.create_collection('twitter_temp', capped=True, size=10000000)

        self.temp_collection = db['twitter_temp']

    def launch(self):

        while True:

            cursor = self.temp_collection.find(cursor_type=pymongo.CursorType.TAILABLE_AWAIT)

            while cursor.alive:
                try:
                    message = cursor.next()
                    # logging.info(message)

                    twitter_scorer = TwitterScorer(message['account_id'])

                    ud = twitter_scorer.getUpdateDocument()
                    logging.info(ud)

                except StopIteration:
                    logging.info('wait')
                    time.sleep(1)


if __name__ == '__main__':
    launcher = Trigger()
    launcher.launch()


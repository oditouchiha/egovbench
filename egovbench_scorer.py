import logging
import math
try:
    import simplejson as json
except ImportError:
    import json

from configparser import SafeConfigParser

from egovbench_mongo import TwitterMongoConnector, YoutubeMongoConnector, FacebookMongoConnector


class EIScorer():

    def __init__(
        self,
        filterDict,
        MongoConnector,
        postLikeCountKey,
        postCommentCountKey,
        postReshareCountKey,
        accountIDKey,
        postTypeKey
    ):

        self.filter_dict = filterDict

        self.mongo_connector_class = MongoConnector

        self.post_likeCount_key = postLikeCountKey
        self.post_commentCount_key = postCommentCountKey
        self.post_reshareCount_key = postReshareCountKey

        self.account_id_key = accountIDKey
        self.post_type_key = postTypeKey

    def prompt(self, texts):
        logging.info('[EGOVBENCH_SCORER]>' + ' ' + texts)

    def getPostCountWithFieldNotZero(self, field):

        postCountWithFieldNotZero = self.mongo_connector_class.getPostCountWithFieldNotZero(self.filter_dict, field)
        return postCountWithFieldNotZero

    def getFieldSum(self, field):

        fieldSum = self.mongo_connector_class.getFieldSum(self.filter_dict, field)
        return fieldSum

    def getPostCount(self):

        postCount = self.mongo_connector_class.getPostCount(self.filter_dict)
        return postCount

    def getFollowerCount(self):

        if self.account_id_key in self.filter_dict and self.post_type_key in self.filter_dict:
            followerCount = self.mongo_connector_class.getFollowerCount(self.filter_dict[self.account_id_key])

        elif self.account_id_key in self.filter_dict and self.post_type_key not in self.filter_dict:
            followerCount = self.mongo_connector_class.getFollowerCount(self.filter_dict[self.account_id_key])

        elif self.account_id_key not in self.filter_dict and self.post_type_key in self.filter_dict:
            followerCount = self.mongo_connector_class.getFollowerSum()

        return followerCount

    def getCommentCount(self):

        commentCount = self.mongo_connector_class.getCommentCount(self.filter_dict)
        return commentCount

    def getMaxFollowerCount(self):

        collection_maxsubscriberCount = self.mongo_connector_class.getMaxFollowerCount()
        return collection_maxsubscriberCount

    def getMinFollowerCount(self):

        collection_minsubscriberCount = self.mongo_connector_class.getMinFollowerCount()
        return collection_minsubscriberCount

    def getAccountMaxEIScore(self):

        eimax = self.mongo_connector_class.getAccountMaxEIScore()
        return eimax

    def getAccountMinEIScore(self):

        eimin = self.mongo_connector_class.getAccountMinEIScore()
        return eimin

    def getAccountPostTypeMaxEIScore(self):

        apteimax = self.mongo_connector_class.getAccountPostTypeMaxEIScore(self.filter_dict[self.account_id_key])
        return apteimax

    def getAccountPostTypeMinEIScore(self):

        apteimin = self.mongo_connector_class.getAccountPostTypeMinEIScore(self.filter_dict[self.account_id_key])
        return apteimin

    def getPostTypeMaxEIScore(self):

        pteimax = self.mongo_connector_class.getPostTypeMaxEIScore()
        return pteimax

    def getPostTypeMinEIScore(self):

        pteimin = self.mongo_connector_class.getPostTypeMinEIScore()
        return pteimin

    #                          S C O R I N G   T E M P L A T E
    # ------------------------------------------------------------------------------------------

    def createSP1(self, field):

        ''' Menghitung sub-parameter pertama '''

        field_notzeroCount = self.getPostCountWithFieldNotZero(field)
        postCount = self.getPostCount()

        try:
            sub_parameter_1 = field_notzeroCount / postCount
        except ZeroDivisionError as er:
            logging.warning(er)
            sub_parameter_1 = None

        return sub_parameter_1

    def createSP2(self, field):

        ''' Menghitung sub-parameter kedua '''

        fieldCount = self.getFieldSum(field)
        postCount = self.getPostCount()

        try:
            sub_parameter_2 = fieldCount / postCount
        except ZeroDivisionError as er:
            logging.warning(er)
            sub_parameter_2 = None

        return sub_parameter_2

    def createSP3(self, field):

        ''' Menghitung sub-parameter ketiga '''

        sub_parameter_2 = self.createSP2(field)
        followerCount = self.getFollowerCount()

        try:
            sub_parameter_3 = (sub_parameter_2 / followerCount) * 1000
        except (ZeroDivisionError, KeyError, ValueError, TypeError) as er:
            logging.warning(er)
            sub_parameter_3 = None

        return sub_parameter_3

    #                                   P A R A M E T E R
    # ------------------------------------------------------------------------------------------

    def getP1(self):
        '''Menghitung popularity-1'''
        self.prompt('{} Calculating Popularity-1 . . .'.format(json.dumps(self.filter_dict)))
        p1 = self.createSP1(self.post_likeCount_key)
        self.prompt('{} Popularity-1 = '.format(json.dumps(self.filter_dict)) + str(p1))
        return p1

    def getP2(self):
        '''Menghitung popularity-2'''
        self.prompt('{} Calculating Popularity-2 . . .'.format(json.dumps(self.filter_dict)))
        p2 = self.createSP2(self.post_likeCount_key)
        self.prompt('{} Popularity-2 = '.format(json.dumps(self.filter_dict)) + str(p2))
        return p2

    def getP3(self):
        '''Menghitung popularity-3'''
        self.prompt('{} Calculating Popularity-3 . . .'.format(json.dumps(self.filter_dict)))
        p3 = self.createSP3(self.post_likeCount_key)
        self.prompt('{} Popularity-3 = '.format(json.dumps(self.filter_dict)) + str(p3))
        return p3

    def getC1(self):
        '''Menghitung commitment-1'''
        self.prompt('{} Calculating Commitment-1 . . .'.format(json.dumps(self.filter_dict)))
        c1 = self.createSP1(self.post_commentCount_key)
        self.prompt('{} Commitment-1 = '.format(json.dumps(self.filter_dict)) + str(c1))
        return c1

    def getC2(self):
        '''Menghitung commitment-2'''
        self.prompt('{} Calculating Commitment-2 . . .'.format(json.dumps(self.filter_dict)))
        c2 = self.createSP2(self.post_commentCount_key)
        self.prompt('{} Commitment-2 = '.format(json.dumps(self.filter_dict)) + str(c2))
        return c2

    def getC3(self):
        '''Menghitung commitment-3'''
        self.prompt('{} Calculating Commitment-3 . . .'.format(json.dumps(self.filter_dict)))
        c3 = self.createSP3(self.post_commentCount_key)
        self.prompt('{} Commitment-3 = '.format(json.dumps(self.filter_dict)) + str(c3))
        return c3

    def getV1(self):
        '''Menghitung virality-1'''
        self.prompt('{} Calculating Virality-1 . . .'.format(json.dumps(self.filter_dict)))
        v1 = self.createSP1(self.post_reshareCount_key)
        self.prompt('{} Virality-1 = '.format(json.dumps(self.filter_dict)) + str(v1))
        return v1

    def getV2(self):
        '''Menghitung virality-2'''
        self.prompt('{} Calculating Virality-2 . . .'.format(json.dumps(self.filter_dict)))
        v2 = self.createSP2(self.post_reshareCount_key)
        self.prompt('{} Virality-2 = '.format(json.dumps(self.filter_dict)) + str(v2))
        return v2

    def getV3(self):
        '''Menghitung virality-3'''
        self.prompt('{} Calculating Virality-3 . . .'.format(json.dumps(self.filter_dict)))
        v3 = self.createSP3(self.post_reshareCount_key)
        self.prompt('{} Virality-3 = '.format(json.dumps(self.filter_dict)) + str(v3))
        return v3

    def getEngagementIndexScore(self):
        '''Menghitung Engagement Index Score'''
        self.prompt('{} Calculating Engagement Index Score . . .'.format(json.dumps(self.filter_dict)))

        try:
            e = self.getP3() + self.getC3() + self.getV3()
            e = math.log10(1 + e)

            # if self.post_type_key in self.filter_dict:
            #     e = e * 1000000000

        except (ValueError, KeyError, TypeError) as er:
            logging.warning(er)
            e = None

        self.prompt('{} Engagement Index Score = '.format(json.dumps(self.filter_dict)) + str(e))
        return e

    #                                N O R M A L I S A S I
    # ------------------------------------------------------------------------------------------

    def normalize(self, accountFieldCount, collectionMaxFieldCount, collectionMinFieldCount):

        ''' Normalisasi data dengan max-min normalization '''

        account_fieldCount = accountFieldCount

        collection_maxfieldCount = collectionMaxFieldCount
        collection_minfieldCount = collectionMinFieldCount

        try:
            result = (account_fieldCount - collection_minfieldCount) / (collection_maxfieldCount - collection_minfieldCount)
        except (ZeroDivisionError, ValueError, KeyError, TypeError) as er:
            logging.warning(er)
            result = None

        return result

    def getAccountNormalizedEngagementIndexScore(self):

        self.prompt("{} Normalizing Engagement Index Score . . .".format(json.dumps(self.filter_dict)))

        normalized_e = self.normalize(self.getEngagementIndexScore(), self.getAccountMaxEIScore(), self.getAccountMinEIScore())

        self.prompt("{} Normalized Engagement Index Score = ".format(json.dumps(self.filter_dict)) + str(normalized_e))
        return normalized_e

    def getAccountPostTypeNormalizedEngagementIndexScore(self):

        self.prompt("{} Normalizing Engagement Index Score . . .".format(json.dumps(self.filter_dict)))

        normalized_e = self.normalize(self.getEngagementIndexScore(), self.getAccountPostTypeMaxEIScore(), self.getAccountPostTypeMinEIScore())

        self.prompt("{} Normalized Engagement Index Score = ".format(json.dumps(self.filter_dict)) + str(normalized_e))
        return normalized_e

    def getPostTypeNormalizedEngagementIndexScore(self):

        self.prompt("{} Normalizing Engagement Index Score . . .".format(json.dumps(self.filter_dict)))

        normalized_e = self.normalize(self.getEngagementIndexScore(), self.getPostTypeMaxEIScore(), self.getPostTypeMinEIScore())

        self.prompt("{} Normalized Engagement Index Score = ".format(json.dumps(self.filter_dict)) + str(normalized_e))
        return normalized_e


class TwitterScorer(EIScorer):

    def __init__(self, filterDict):
        super(TwitterScorer, self).__init__(
            filterDict,
            TwitterMongoConnector(),
            'tweet_favoriteCount',
            'tweet_replyCount',
            'tweet_retweetCount',
            'account_id',
            'tweet_type'
        )

        self.filter_dict = filterDict
        self.tmc = TwitterMongoConnector()

    def getAccountStatisticDocument(self):

        self.prompt('{} Creating statistic document . . .'.format(json.dumps(self.filter_dict)))

        update_document = {}

        update_document['account_id'] = self.filter_dict['account_id'].lower()
        update_document['account_followerCount'] = self.getFollowerCount()

        update_document['result.statistics'] = {}
        update_document['result.statistics']['tweetCount'] = self.getPostCount()
        update_document['result.statistics']['favoriteCount'] = self.getFieldSum('tweet_favoriteCount')
        update_document['result.statistics']['replyCount'] = self.getFieldSum('tweet_replyCount')
        update_document['result.statistics']['retweetCount'] = self.getFieldSum('tweet_retweetCount')

        self.prompt('{} Statistic document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getAccountScoreDocument(self):

        update_document = {}

        self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

        update_document['account_id'] = self.filter_dict['account_id'].lower()

        update_document['result.scores'] = {}

        update_document['result.scores']['popularity_favoriteScore'] = {}
        update_document['result.scores']['popularity_favoriteScore']['popularity_favoriteScore_1'] = self.getP1()
        update_document['result.scores']['popularity_favoriteScore']['popularity_favoriteScore_3'] = self.getP3()

        update_document['result.scores']['commitment_replyScore'] = {}
        update_document['result.scores']['commitment_replyScore']['commitment_replyScore_1'] = self.getC1()
        update_document['result.scores']['commitment_replyScore']['commitment_replyScore_3'] = self.getC3()

        update_document['result.scores']['virality_retweetScore'] = {}
        update_document['result.scores']['virality_retweetScore']['virality_retweetScore_1'] = self.getV1()
        update_document['result.scores']['virality_retweetScore']['virality_retweetScore_3'] = self.getV3()

        update_document['result.scores']['engagement_index_score'] = self.getEngagementIndexScore()

        engagement_index_score_normalized = self.getAccountNormalizedEngagementIndexScore()
        update_document['result.scores']['engagement_index_score_normalized'] = engagement_index_score_normalized * 100 if engagement_index_score_normalized else None

        self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getAccountPostTypeScoreDocument(self):

        update_document = {}

        post_types = self.tmc.getPostTypeDistinct('tweet_type')

        for post_type in post_types:

            self.filter_dict.pop('tweet_type', None)
            posttypeattribute = {'tweet_type': post_type}
            posttypeattribute.update(self.filter_dict)

            super(TwitterScorer, self).__init__(
                posttypeattribute,
                TwitterMongoConnector(),
                'tweet_favoriteCount',
                'tweet_replyCount',
                'tweet_retweetCount',
                'account_id',
                'tweet_type'
            )

            self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

            update_document['account_id'] = posttypeattribute['account_id'].lower()

            update_document['post_type_result.%s.scores' % (post_type)] = {}
            update_document['post_type_result.%s.scores' % (post_type)]['engagement_index_score'] = self.getEngagementIndexScore()

            self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getPostTypeStatisticDocument(self):

        update_document = {}

        post_types = self.tmc.getPostTypeDistinct('tweet_type')

        for post_type in post_types:

            posttypeattribute = {'tweet_type': post_type}

            super(TwitterScorer, self).__init__(
                posttypeattribute,
                TwitterMongoConnector(),
                'tweet_favoriteCount',
                'tweet_replyCount',
                'tweet_retweetCount',
                'account_id',
                'tweet_type'
            )

            self.prompt('{} Creating statistic document . . .'.format(json.dumps(self.filter_dict)))

            update_document['_id'] = posttypeattribute['tweet_type']

            update_document['result.statistics'] = {}
            update_document['result.statistics']['tweetCount'] = self.getPostCount()

            self.prompt('{} Statistic document created!'.format(json.dumps(self.filter_dict)))

            self.mongo_connector_class.updatePostTypeResult(update_document)

    def getPostTypeScoreDocument(self):

        update_document = {}

        post_types = self.tmc.getPostTypeDistinct('tweet_type')

        for post_type in post_types:

            posttypeattribute = {'tweet_type': post_type}

            super(TwitterScorer, self).__init__(
                posttypeattribute,
                TwitterMongoConnector(),
                'tweet_favoriteCount',
                'tweet_replyCount',
                'tweet_retweetCount',
                'account_id',
                'tweet_type'
            )

            self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

            update_document['_id'] = posttypeattribute['tweet_type']

            update_document['result.scores'] = {}
            update_document['result.scores']['engagement_index_score'] = self.getEngagementIndexScore()

            self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

            self.mongo_connector_class.updatePostTypeResult(update_document)


class YoutubeScorer(EIScorer):

    def __init__(self, filterDict):
        super(YoutubeScorer, self).__init__(
            filterDict,
            YoutubeMongoConnector(),
            'video_likeCount',
            'video_commentCount',
            '',
            'channel_id',
            ''
        )

        self.filter_dict = filterDict
        self.ymc = YoutubeMongoConnector()

    def getMaxViewCount(self):

        ''' Mengambil fungsi getMaxViewCount() dari MongoConnector'''

        collection_maxviewCount = self.mongo_connector_class.getMaxViewCount()
        return collection_maxviewCount

    def getMinViewCount(self):

        ''' Mengambil fungsi getMinViewCount() dari MongoConnector'''

        collection_minviewCount = self.mongo_connector_class.getMinViewCount()
        return collection_minviewCount

    def createSP3(self, field):

        ''' Menghitung sub-parameter ketiga '''

        normalized_subscriberCount = self.normalize(
                self.getFollowerCount(),
                self.getMaxFollowerCount(),
                self.getMinFollowerCount()
        )

        normalized_viewCount = self.normalize(
            self.getFieldSum('video_viewCount'),
            self.getMaxViewCount(),
            self.getMinViewCount()
        )
        self.prompt('{} Normalized viewCount = '.format(json.dumps(self.filter_dict)) + str(normalized_viewCount))

        sub_parameter_2 = self.createSP2(field)

        try:
            followerScore = (normalized_subscriberCount * 0.5) + (normalized_viewCount * 0.5)
            self.prompt('{} Follower Score = '.format(json.dumps(self.filter_dict)) + str(followerScore))
        except (ZeroDivisionError, KeyError, ValueError, TypeError) as er:
            logging.warning(er)
            followerScore = None

        try:
            sub_parameter_3 = (sub_parameter_2 / followerScore)
        except (ZeroDivisionError, KeyError, ValueError, TypeError) as er:
            logging.warning(er)
            sub_parameter_3 = None

        return sub_parameter_3

    def getV1(self):
        ''' Exception untuk parameter virality sebab tidak ada data reshare dari Youtube '''
        return logging.error('No reshareCount data for youtube scoring')

    def getV2(self):
        ''' Exception untuk parameter virality sebab tidak ada data reshare dari Youtube '''
        return logging.error('No reshareCount data for youtube scoring')

    def getV3(self):
        ''' Exception untuk parameter virality sebab tidak ada data reshare dari Youtube '''
        return logging.error('No reshareCount data for youtube scoring')

    def getEngagementIndexScore(self):

        '''Menghitung Engagement Index Score (meng-override method getEngagementIndexScore() dari parent class EIScorer())'''

        self.prompt('{} Calculating Engagement Index Score . . .'.format(self.filter_dict))

        try:
            e = self.getP3() + self.getC3()
            e = math.log10(1 + e)
        except (ValueError, KeyError, TypeError) as er:
            logging.warning(er)
            e = None

        self.prompt('{} Engagement Index Score = '.format(json.dumps(self.filter_dict)) + str(e))
        return e

    def getRatingScore(self):

        '''Menghitung Rating Score'''

        self.prompt('{} Calculating Rating Score . . .'.format(json.dumps(self.filter_dict)))

        like_count = self.getFieldSum('video_likeCount')
        dislike_count = self.getFieldSum('video_dislikeCount')

        try:
            rating_score = like_count / (dislike_count + like_count)
        except ZeroDivisionError as er:
            logging.warning(er)
            rating_score = None

        self.prompt('{} Rating Score = '.format(json.dumps(self.filter_dict)) + str(rating_score))

        return rating_score

    def getStatisticDocument(self):

        self.prompt('{} Creating statistic document . . .'.format(json.dumps(self.filter_dict)))

        update_document = {}

        update_document['channel_id'] = self.filter_dict['channel_id'].lower()
        update_document['channel_subscriberCount'] = self.getFollowerCount()

        update_document['result.statistics'] = {}
        update_document['result.statistics']['videoCount'] = self.getPostCount()
        update_document['result.statistics']['likeCount'] = self.getFieldSum('video_likeCount')
        update_document['result.statistics']['dislikeCount'] = self.getFieldSum('video_dislikeCount')
        update_document['result.statistics']['commentCount'] = self.getFieldSum('video_commentCount')
        update_document['result.statistics']['viewCount'] = self.getFieldSum('video_viewCount')

        self.prompt('{} Statistic document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getScoreDocument(self):

        self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

        update_document = {}

        update_document['channel_id'] = self.filter_dict['channel_id'].lower()

        update_document['result.scores'] = {}

        update_document['result.scores']['popularity_likeScore'] = {}
        update_document['result.scores']['popularity_likeScore']['popularity_likeScore_1'] = self.getP1()
        update_document['result.scores']['popularity_likeScore']['popularity_likeScore_3'] = self.getP3()

        update_document['result.scores']['commitment_commentScore'] = {}
        update_document['result.scores']['commitment_commentScore']['commitment_commentScore_1'] = self.getC1()
        update_document['result.scores']['commitment_commentScore']['commitment_commentScore_3'] = self.getC3()

        update_document['result.scores']['engagement_index_score'] = self.getEngagementIndexScore()

        engagement_index_score_normalized = self.getAccountNormalizedEngagementIndexScore()
        update_document['result.scores']['engagement_index_score_normalized'] = engagement_index_score_normalized * 100 if engagement_index_score_normalized else None

        rating_score = self.getRatingScore()
        update_document['result.scores']['rating_score'] = rating_score * 100 if rating_score else None

        self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

        return update_document


class FacebookScorer(EIScorer):

    def __init__(self, filterDict, confFile='/home/addi/egovbench/apps/pythons/egovbench_config.ini'):
        super(FacebookScorer, self).__init__(
            filterDict,
            FacebookMongoConnector(),
            'post_reactions.like',
            'post_commentCount',
            'post_shareCount',
            'page_id',
            'post_type'
        )

        self.filter_dict = filterDict
        self.fmc = FacebookMongoConnector()

        # Mengambil skor sentimen reaction dari file egovbench_config.ini
        self.confparser = SafeConfigParser()
        self.confparser.read(confFile)

    def getReactionScore(self, reaction):

        self.prompt('{} Calculating {} reaction score . . .'.format(json.dumps(self.filter_dict), reaction))

        ''' Template scoring untuk menghitung Reaction Score '''

        reactionCount = self.getFieldSum('post_reactions.{}'.format(reaction))

        total_reactionCount = self.getFieldSum('post_reactions.like') \
                            + self.getFieldSum('post_reactions.angry') \
                            + self.getFieldSum('post_reactions.wow') \
                            + self.getFieldSum('post_reactions.sad') \
                            + self.getFieldSum('post_reactions.haha') \
                            + self.getFieldSum('post_reactions.love')

        sentiment_score = self.confparser.get('SentimentScoreConfig', reaction)

        try:
            reaction_score = float(sentiment_score) * (reactionCount / total_reactionCount)
        except ZeroDivisionError as er:
            logging.warning(er)
            reaction_score = None

        self.prompt('{} {} reaction score: {}'.format(json.dumps(self.filter_dict), reaction, reaction_score))

        return reaction_score

    def getLikeScore(self):
        ''' Menghitung skor reaction 'like' '''
        return self.getReactionScore('like')

    def getAngryScore(self):
        ''' Menghitung skor reaction 'angry' '''
        return self.getReactionScore('angry')

    def getWowScore(self):
        ''' Menghitung skor reaction 'wow' '''
        return self.getReactionScore('wow')

    def getSadScore(self):
        ''' Menghitung skor reaction 'sad' '''
        return self.getReactionScore('sad')

    def getHahaScore(self):
        ''' Menghitung skor reaction 'haha' '''
        return self.getReactionScore('haha')

    def getLoveScore(self):
        ''' Menghitung skor reaction 'love' '''
        return self.getReactionScore('love')

    def getTotalReactionScore(self):

        ''' Menghitung total reaction score '''

        self.prompt('{} Calculating Reaction Score . . .'.format(json.dumps(self.filter_dict)))

        try:
            total_reaction_score = self.getLikeScore() \
                                 + self.getAngryScore() \
                                 + self.getWowScore() \
                                 + self.getSadScore() \
                                 + self.getHahaScore() \
                                 + self.getLoveScore()

        except (ValueError, KeyError, TypeError) as er:
            logging.warning(er)
            total_reaction_score = None

        self.prompt('{} Reaction Score: '.format(json.dumps(self.filter_dict)) + str(total_reaction_score))

        return total_reaction_score

    def getAccountStatisticDocument(self):

        self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

        update_document = {}

        update_document['page_id'] = self.filter_dict['page_id'].lower()
        update_document['page_fanCount'] = self.getFollowerCount()

        update_document['result.statistics'] = {}
        update_document['result.statistics']['postCount'] = self.getPostCount()
        update_document['result.statistics']['commentCount'] = self.getFieldSum('post_commentCount')
        update_document['result.statistics']['reshareCount'] = self.getFieldSum('post_shareCount')

        update_document['result.statistics']['reactions'] = {}
        update_document['result.statistics']['reactions']['like'] = self.getFieldSum('post_reactions.like')
        update_document['result.statistics']['reactions']['angry'] = self.getFieldSum('post_reactions.angry')
        update_document['result.statistics']['reactions']['wow'] = self.getFieldSum('post_reactions.wow')
        update_document['result.statistics']['reactions']['sad'] = self.getFieldSum('post_reactions.sad')
        update_document['result.statistics']['reactions']['haha'] = self.getFieldSum('post_reactions.haha')
        update_document['result.statistics']['reactions']['love'] = self.getFieldSum('post_reactions.love')

        self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getAccountScoreDocument(self):

        self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

        update_document = {}

        update_document['page_id'] = self.filter_dict['page_id'].lower()

        update_document['result.scores'] = {}

        update_document['result.scores']['popularity_likeScore'] = {}
        update_document['result.scores']['popularity_likeScore']['popularity_likeScore_1'] = self.getP1()
        update_document['result.scores']['popularity_likeScore']['popularity_likeScore_3'] = self.getP3()

        update_document['result.scores']['commitment_commentScore'] = {}
        update_document['result.scores']['commitment_commentScore']['commitment_commentScore_1'] = self.getC1()
        update_document['result.scores']['commitment_commentScore']['commitment_commentScore_3'] = self.getC3()

        update_document['result.scores']['virality_shareScore'] = {}
        update_document['result.scores']['virality_shareScore']['virality_shareScore_1'] = self.getV1()
        update_document['result.scores']['virality_shareScore']['virality_shareScore_3'] = self.getV3()

        update_document['result.scores']['engagement_index_score'] = self.getEngagementIndexScore()

        engagement_index_score_normalized = self.getAccountNormalizedEngagementIndexScore()
        update_document['result.scores']['engagement_index_score_normalized'] = engagement_index_score_normalized * 100 if engagement_index_score_normalized else None

        update_document['result.scores']['reaction_score'] = {}
        update_document['result.scores']['reaction_score']['total'] = self.getTotalReactionScore()

        self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getAccountPostTypeScoreDocument(self):

        update_document = {}

        post_types = self.fmc.getPostTypeDistinct('post_type')

        for post_type in post_types:

            self.filter_dict.pop('post_type', None)
            posttypeattribute = {'post_type': post_type}
            posttypeattribute.update(self.filter_dict)

            super(FacebookScorer, self).__init__(
                posttypeattribute,
                FacebookMongoConnector(),
                'post_reactions.like',
                'post_commentCount',
                'post_shareCount',
                'page_id',
                'post_type'
            )

            self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

            update_document['page_id'] = self.filter_dict['page_id'].lower()

            update_document['post_type_result.%s.scores' % (post_type)] = {}
            update_document['post_type_result.%s.scores' % (post_type)]['engagement_index_score'] = self.getEngagementIndexScore()

            self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

        return update_document

    def getPostTypeStatisticDocument(self):

        update_document = {}

        post_types = self.fmc.getPostTypeDistinct('post_type')

        for post_type in post_types:

            posttypeattribute = {'post_type': post_type}

            super(FacebookScorer, self).__init__(
                posttypeattribute,
                FacebookMongoConnector(),
                'post_reactions.like',
                'post_commentCount',
                'post_shareCount',
                'page_id',
                'post_type'
            )

            self.prompt('{} Creating statistic document . . .'.format(json.dumps(self.filter_dict)))

            update_document['_id'] = posttypeattribute['post_type']

            update_document['result.statistics'] = {}
            update_document['result.statistics']['postCount'] = self.getPostCount()

            self.prompt('{} Statistic document created!'.format(json.dumps(self.filter_dict)))

            self.mongo_connector_class.updatePostTypeResult(update_document)

    def getPostTypeScoreDocument(self):

        update_document = {}

        post_types = self.fmc.getPostTypeDistinct('post_type')

        for post_type in post_types:

            posttypeattribute = {'post_type': post_type}

            super(FacebookScorer, self).__init__(
                posttypeattribute,
                FacebookMongoConnector(),
                'post_reactions.like',
                'post_commentCount',
                'post_shareCount',
                'page_id',
                'post_type'
            )

            self.prompt('{} Creating score document . . .'.format(json.dumps(self.filter_dict)))

            update_document['_id'] = posttypeattribute['post_type']

            update_document['result.scores'] = {}
            update_document['result.scores']['engagement_index_score'] = self.getEngagementIndexScore()

            self.prompt('{} Score document created!'.format(json.dumps(self.filter_dict)))

            self.mongo_connector_class.updatePostTypeResult(update_document)

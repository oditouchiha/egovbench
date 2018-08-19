import pymongo
import logging
import json
import datetime
import re


class MongoConnector():

    '''
        Kelas ini berfungsi untuk mengkomunikasikan MongoDB dengan sistem
    '''

    def __init__(
        self,
        accountCollection,
        postCollection,
        commentCollection,
        tempCollection,
        accountResultCollection,
        postTypeResultCollection,
        accountTypeKey,
        accountIDKey,
        postIDKey,
        postTypeKey,
        commentIDKey,
        accountFollowerKey,
        resmiKey,
        database='egovbench'
    ):
        '''
            Inisiasi :

            * database:             nama database di MongoDB (default: egovbench_sosmed)

            * account_collection:   collection yang terdiri dari dokumen milik akun
            * post_collection:      collection yang terdiri dari dokumen milik post
            * comment_collection:   collection yang terdiri dari dokumen milik comment
            * temp_collection:      collection untuk aktivasi trigger / tailable cursor

            * accountResultCollection: collection untuk menyimpan hasil scoring milik akun
            * postTypeResultCollection: collection untuk menyimpan hasil scoring milik jenis post

            * accountTypeKey:       key dari tipe akun (ex: channel_type)
            * accountIDKey:         key dari id akun dari sosmed (ex: page_id)
            * postIDKey:            key dari id post dari sosmed (ex: tweet_id)
            * postTypeKey:          key dari jenis post
            * commentIDKey:         key dari id comment

            * accountFollowerKey: key dari jumlah follower (ex: account_followerCount)

        '''

        client = pymongo.MongoClient(
            '127.0.0.1',
            username='egovadmin',
            password='2B?Zge36sF3Ag9@Z',
            authMechanism='SCRAM-SHA-1'
        )
        self.database = client[database]

        self.accounts_collection = self.database['listpemda']

        self.account_collection = self.database[accountCollection]
        self.post_collection = self.database[postCollection]
        self.comment_collection = self.database[commentCollection]
        self.temp_collection = self.database[tempCollection]

        self.post_type_result_collection = self.database[postTypeResultCollection]
        self.account_result_collection = self.database[accountResultCollection]

        self.pemda_id_key = 'pemda_id'
        self.account_type_key = accountTypeKey
        self.account_id_key = accountIDKey
        self.post_id_key = postIDKey
        self.post_type_key = postTypeKey
        self.comment_id_key = commentIDKey

        self.account_follower_key = accountFollowerKey

        self.resmi_key = resmiKey

        self.current_date_yyyymmdd = datetime.datetime.today().strftime('%Y-%m-%d')

    def prompt(self, texts):
        logging.info('[EGOVBENCH_MONGO]>' + ' ' + texts)

    #                                       C R A W L E R
    # ------------------------------------------------------------------------------------------

    def collectAccounts(self, filterdict={}):

        self.prompt('(accounts collection: {}) Collecting account from database . . .'.format(self.accounts_collection.name))

        accounts = self.accounts_collection.find(filterdict, no_cursor_timeout=True).batch_size(5)

        return accounts

    def checkAccount(self, account_id):

        '''
            Mengecek apakah pemda_id telah memiliki account_id di database.
        '''

        self.prompt('({}: {}) Checking account . . .'.format(self.account_id_key, account_id))

        account_id = account_id.lower()

        try:
            result = self.account_collection.find({'_id': account_id})
            result = list(result)

            account_exist = True if result else False
            self.prompt('({}: {}) Account status : {}'.format(self.account_id_key, account_id, 'Exist' if account_exist else 'Not Exist !'))

            return account_exist

        except Exception as e:
            logging.warning(e)

    def updatePost(self, postDocument):

        self.prompt('({}: {}) Updating post please wait . . .'.format(self.post_id_key, postDocument['_id']))

        try:
            self.post_collection.update_one(
                {
                    '_id': postDocument['_id']
                },
                {
                    '$set': postDocument
                },
                upsert=True
            )
            self.prompt('({}: {}) Post updated !'.format(self.post_id_key, postDocument['_id']))

        except Exception as e:
            logging.warning(e)

    def updateComment(self, commentDocument):

        self.prompt('({}: {}) Updating comment please wait . . .'.format(self.comment_id_key, commentDocument['_id']))

        try:
            self.comment_collection.update_one(
                {
                    '_id': commentDocument['_id']
                },
                {
                    '$set': commentDocument
                },
                upsert=True
            )
            self.prompt('({}: {}) Comment updated !'.format(self.comment_id_key, commentDocument['_id']))

        except Exception as e:
            logging.warning(e)

    def updateAccount(self, accountDocument):

        '''
            Meng-update post pada post collection, dengan filter pemda_id_key (dan account_type_key bila ada) dari accountDocument.
            Bila filter tidak terpenuhi, akan menambah document baru ke dalam post collection.
        '''

        self.prompt('({}: {}) Updating Account please wait . . .'.format(self.account_id_key, accountDocument['_id']))

        self.insertTemp(accountDocument)

        if self.account_type_key in accountDocument:
            accountDocument.pop(self.account_type_key, None)

        try:
            self.account_collection.update_one(
                {
                    '_id': accountDocument['_id']
                },
                {
                    '$set': accountDocument
                },
                upsert=True
            )
            self.prompt('({}: {}) Account updated !'.format(self.account_id_key, accountDocument['_id']))

        except Exception as e:
            logging.warning(e)

    def insertTemp(self, document):

        '''
            Meng-insert document ke dalam temp collection untuk mengaktifkan trigger.
        '''

        self.prompt('({}: {}) Creating temp file for trigger . . .'.format(self.account_id_key, document['_id']))

        temp_document = document.copy()
        temp_document['id'] = temp_document['_id']
        temp_document.pop('_id', None)

        try:
            self.temp_collection.insert_one(temp_document)

            self.prompt('({}: {}) Temp file created!'.format(self.account_id_key, document['_id']))

        except Exception as e:
            logging.warning(e)

    #                                       T R I G G E R
    # ------------------------------------------------------------------------------------------

    def resetTemp(self):

        '''
            Mereset isi temp collection
        '''

        self.prompt('(temp_collection: {}) Resetting temp collection . . .'.format(self.temp_collection.name))

        try:
            self.database.drop_collection(self.temp_collection.name)
            self.database.create_collection(self.temp_collection.name, capped=True, size=10000000)

            self.prompt('(temp_collection: {}) Temp collection resetted!'.format(self.temp_collection.name))

        except Exception as e:
            logging.warning(e)

    def activateTailableCursor(self):

        '''
            Mengaktifkan tailable cursor pada trigger.
            Tailable cursor mampu membaca input data yang baru saja masuk ke collection.
        '''

        self.prompt('(temp_collection: {}) Activating tailable cursor . . .'.format(self.temp_collection.name))

        try:
            cursor = self.temp_collection.find(cursor_type=pymongo.CursorType.TAILABLE_AWAIT)
            return cursor

        except Exception as e:
            logging.warning(e)

    def updateAccountResult(self, updateDocument):

        '''
            Mengupdate score pada account collection
        '''

        self.prompt('({}: {}) Updating result . . .'.format(self.account_id_key, updateDocument[self.account_id_key]))

        updateDocument['result_createdDate'] = self.current_date_yyyymmdd

        try:
            self.account_result_collection.update_many(
                {
                    self.account_id_key: updateDocument[self.account_id_key],
                    'result_createdDate': updateDocument['result_createdDate']
                },
                {
                    '$set': updateDocument
                },
                upsert=True
            )

            self.prompt('({}: {}) Result updated!'.format(self.account_id_key, updateDocument[self.account_id_key]))

        except Exception as e:
            logging.warning(e)

    def updatePostTypeResult(self, updateDocument):

        '''
            Mengupdate score pada account collection
        '''

        self.prompt('({}: {}) Updating result . . .'.format(self.post_type_key, updateDocument['_id']))

        try:
            self.post_type_result_collection.update_many(
                {
                    '_id': updateDocument['_id']
                },
                {
                    '$set': updateDocument
                },
                upsert=True
            )

            self.prompt('({}: {}) Result updated!'.format(self.post_type_key, updateDocument['_id']))

        except Exception as e:
            logging.warning(e)

    def updatePemdaScores(self, accountID):

        '''
            Mengupdate score pada accounts collection (listpemda)
        '''

        self.prompt('({}: {}) Finding pemda in {} collection'.format(self.resmi_key, accountID, self.accounts_collection.name))

        filterdict = {self.resmi_key: re.compile(accountID, re.IGNORECASE)}

        accounts = self.collectAccounts(filterdict)

        for account in accounts:

            self.prompt('({}: {}) Updating Pemda Scores . . .'.format('pemda', account['name']))

            facebook_engagement_index_score_normalized = None
            twitter_engagement_index_score_normalized = None
            youtube_engagement_index_score_normalized = None
            facebook_reaction_score = None
            youtube_rating_score = None

            if account['facebook_resmi']:
                facebook_accounts_result_collection = self.database['facebook_accounts_result']
                facebook_result = facebook_accounts_result_collection.find_one({'page_id': account['facebook_resmi'].lower()}, sort=[("result_createdDate", pymongo.DESCENDING)])
                facebook_engagement_index_score_normalized = facebook_result['result']['scores']['engagement_index_score_normalized']
                facebook_reaction_score = facebook_result['result']['scores']['reaction_score']['total']

            if account['twitter_resmi']:
                twitter_accounts_result_collection = self.database['twitter_accounts_result']
                twitter_result = twitter_accounts_result_collection.find_one({'account_id': account['twitter_resmi'].lower()}, sort=[("result_createdDate", pymongo.DESCENDING)])
                twitter_engagement_index_score_normalized = twitter_result['result']['scores']['engagement_index_score_normalized']

            if account['youtube_resmi']:
                youtube_accounts_result_collection = self.database['youtube_accounts_result']
                youtube_result = youtube_accounts_result_collection.find_one({'channel_id': account['youtube_resmi'].lower()}, sort=[("result_createdDate", pymongo.DESCENDING)])
                youtube_engagement_index_score_normalized = youtube_result['result']['scores']['engagement_index_score_normalized']
                youtube_rating_score = youtube_result['result']['scores']['rating_score']

            engagement_index_score_total = None

            list_engagement_index_score_normalized = []
            list_engagement_index_score_normalized.extend([facebook_engagement_index_score_normalized, twitter_engagement_index_score_normalized, youtube_engagement_index_score_normalized])

            if not all(x is None for x in list_engagement_index_score_normalized):
                engagement_index_score_total = facebook_engagement_index_score_normalized if facebook_engagement_index_score_normalized else 0 + \
                                           twitter_engagement_index_score_normalized if twitter_engagement_index_score_normalized else 0 + \
                                           youtube_engagement_index_score_normalized if youtube_engagement_index_score_normalized else 0

            update_dict = {}
            update_dict['scores.engagement_index_score'] = engagement_index_score_total
            update_dict['scores.facebook_reaction_score'] = facebook_reaction_score
            update_dict['scores.youtube_rating_score'] = youtube_rating_score

            self.accounts_collection.update_one(
                {
                    '_id': int(account['_id'])
                },
                {
                    '$set': update_dict
                },
                upsert=True
            )

            self.prompt('({}: {}) Pemda scores updated !'.format('pemda', account['name']))

    #                                       S C O R E R
    # ------------------------------------------------------------------------------------------

    def getPostCountWithFieldNotZero(self, filterdict, field):

        '''
            Mendapatkan jumlah post dengan nilai field tidak nol (ex: post memiliki paling tidak satu like)
            yang dimiliki oleh accountID.
        '''

        self.prompt('{} Getting total {}-not-zero . . .'.format(json.dumps(filterdict), field))

        copy = filterdict.copy()

        copy[field] = {
            '$ne': 0
        }

        try:
            account_fieldnotzeroCount = self.post_collection.count(copy)

        except Exception as e:
            logging.warning(e)
            account_fieldnotzeroCount = 0

        self.prompt('{} total {}-not-zero: {}'.format(json.dumps(filterdict), field, str(account_fieldnotzeroCount)))

        return account_fieldnotzeroCount

    def getFieldSum(self, filterdict, field):

        '''
            Mendapatkan jumlah total field yang dimiliki oleh accountID
        '''

        self.prompt('{} Getting total {} . . .'.format(json.dumps(filterdict), field))

        try:
            account_fieldCount = self.post_collection.aggregate(
                [
                    {
                        '$match': filterdict
                    },
                    {
                        '$group': {
                            '_id': '$%s' % (list(filterdict.keys())[0]),
                            'total': {
                                '$sum': '$%s' % (field)
                            }
                        }
                    }
                ]
            )
            account_fieldCount = list(account_fieldCount)[0]['total']

        except Exception as e:
            logging.warning(e)
            account_fieldCount = 0

        self.prompt('{} Total {}: {}'.format(json.dumps(filterdict), field, str(account_fieldCount)))

        return account_fieldCount

    def getPostCount(self, filterdict):

        '''
            Mendapatkan jumlah post yang dimiliki oleh accountID
        '''

        self.prompt('{} Getting total {}. . .'.format(json.dumps(filterdict), self.post_id_key))

        try:
            account_postCount = self.post_collection.count(filterdict)

        except Exception as e:
            logging.warning(e)
            account_postCount = 0

        self.prompt('{} Total {}: {}'.format(json.dumps(filterdict), self.post_id_key, str(account_postCount)))

        return account_postCount

    def getFollowerCount(self, accountID):

        '''
            Mendapatkan jumlah follower yang dimiliki oleh accountID
        '''

        self.prompt('({}: {}) Getting {} . . .'.format(self.account_id_key, accountID, self.account_follower_key))

        try:
            account_followerCount = self.account_collection.find({'_id': accountID}, {self.account_follower_key: 1})
            account_followerCount = account_followerCount[0][self.account_follower_key]

        except Exception as e:
            logging.warning(e)
            account_followerCount = 0

        self.prompt('({}: {}) {}: {}'.format(self.account_id_key, accountID, self.account_follower_key, str(account_followerCount)))

        return account_followerCount

    def getFollowerSum(self):

        '''
            Mendapatkan jumlah follower yang dimiliki oleh accountID
        '''

        self.prompt('Getting total {} . . .'.format(self.account_follower_key))

        try:
            total_followerCount = self.account_collection.aggregate(
                [
                    {
                        '$group': {
                            '_id': 'all accounts',
                            'total': {
                                '$sum': '$%s' % (self.account_follower_key)
                            }
                        }
                    }
                ]
            )
            total_followerCount = list(total_followerCount)[0]['total']

        except Exception as e:
            logging.warning(e)
            total_followerCount = 0

        self.prompt('Total {}: {}'.format(self.account_follower_key, str(total_followerCount)))

        return total_followerCount

    def getCommentCount(self, filterdict):

        '''
            Mendapatkan jumlah post yang dimiliki oleh accountID
        '''

        self.prompt('{} Getting total {}. . .'.format(json.dumps(filterdict), self.comment_id_key))

        try:
            account_postCount = self.comment_collection.count(filterdict)

        except Exception as e:
            logging.warning(e)
            account_postCount = 0

        self.prompt('{} Total {}: {}'.format(json.dumps(filterdict), self.comment_id_key, str(account_postCount)))

        return account_postCount

    def getPostTypeDistinct(self, field):

        self.prompt('(post_collection: {}) Getting post types distinct'.format(self.post_collection.name))

        post_types = []

        try:
            post_types = self.post_collection.distinct(field)
        except Exception as e:
            logging.warning(e)

        post_types_string = ', '.join(post_types)

        self.prompt('(post_collection: {}) Post types acquired: {}'.format(self.post_collection.name, post_types_string))

        return post_types

    def getCommentTypeDistinct(self, field):

        self.prompt('(comment_collection: {}) Getting comment {} distinct'.format(self.comment_collection.name, field))

        comment_types = []

        try:
            comment_types = self.comment_collection.distinct(field)
        except Exception as e:
            logging.warning(e)

        comment_types_string = ', '.join(comment_types)

        self.prompt('(comment_collection: {}) Comment types acquired: {}'.format(self.comment_collection.name, comment_types_string))

        return comment_types

    def getMaxFollowerCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_result_collection: {}) Getting {} max value . . .'.format(self.account_result_collection.name, self.account_follower_key))

        try:
            collection_maxFollowerCount = self.account_result_collection.find({
                'result_createdDate': {'$ne': self.current_date_yyyymmdd},
                self.account_follower_key: {'$exists': True, '$ne': None}
            }).sort(
                self.account_follower_key, -1
            ).limit(1)

            collection_maxFollowerCount = list(collection_maxFollowerCount)[0][self.account_follower_key]
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_result_collection: {}) {} max value: '.format(self.account_result_collection.name, self.account_follower_key) + str(collection_maxFollowerCount))

        return collection_maxFollowerCount

    def getMinFollowerCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_result_collection: {}) Getting {} min value . . .'.format(self.account_result_collection.name, self.account_follower_key))

        try:
            collection_minFollowerCount = self.account_result_collection.find({
                'result_createdDate': {'$ne': self.current_date_yyyymmdd},
                self.account_follower_key: {'$exists': True, '$ne': None}
            }).sort(
                self.account_follower_key, 1
            ).limit(1)

            collection_minFollowerCount = list(collection_minFollowerCount)[0][self.account_follower_key]
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_result_collection: {}) {} min value: '.format(self.account_result_collection.name, self.account_follower_key) + str(collection_minFollowerCount))

        return collection_minFollowerCount

    def getAccountMaxEIScore(self):

        self.prompt("(result_collection: {}) Getting Accounts's Engagement Index Max value".format(self.account_result_collection.name))

        try:
            eimax = self.account_result_collection.find({
            	'result_createdDate': {'$ne': self.current_date_yyyymmdd},
                'result.scores.engagement_index_score': {'$exists': True, '$ne': None}
            }).sort([
                # ['result_createdDate', -1],
                ['result.scores.engagement_index_score', -1]
            ]).limit(1)

            eimax = list(eimax)[0]['result']['scores']['engagement_index_score']

        except KeyError as k:
            logging.warning(k)
            eimax = 1

        except IndexError as i:
            logging.warning(i)
            eimax = 1

        except Exception as e:
            logging.warning(e)

        self.prompt("(result_collection: {}) Accounts's Engagement Index Max value : {}".format(self.account_result_collection.name, str(eimax)))

        return eimax

    def getAccountMinEIScore(self):

        self.prompt("(result_collection: {}) Getting Accounts's Engagement Index Min value".format(self.account_result_collection.name))

        try:
            eimin = self.account_result_collection.find({
            	'result_createdDate': {'$ne': self.current_date_yyyymmdd},
                'result.scores.engagement_index_score': {'$exists': True, '$ne': None}
            }).sort([
                # ['result_createdDate', -1],
                ['result.scores.engagement_index_score', 1]
            ]).limit(1)

            eimin = list(eimin)[0]['result']['scores']['engagement_index_score']

        except KeyError as k:
            logging.warning(k)
            eimin = 0

        except IndexError as i:
            logging.warning(i)
            eimin = 0

        except Exception as e:
            logging.warning(e)

        self.prompt("(result_collection: {}) Accounts's Engagement Index Min value : {}".format(self.account_result_collection.name, str(eimin)))

        return eimin

    def getAccountPostTypeMaxEIScore(self, accountID):

        self.prompt("(result_collection: {}, account_id: {}) Getting Account's Post Types's Engagement Index Max value".format(self.account_result_collection.name, accountID))

        listofscore = []

        try:
            latestresultdocument = self.account_result_collection.find(
                {
                    self.account_id_key: accountID
                }
            ).sort(
                'result_createdDate', -1
            ).limit(1)

            manytypes = list(latestresultdocument)[0]['post_type_result']

            for atype in manytypes:
                listofscore.append(manytypes[atype]['scores']['engagement_index_score'])

            [x for x in listofscore if x is not None]
            listofscore.sort()
            eimax = listofscore[-1]

        except KeyError as k:
            logging.warning(k)
            eimax = 1

        except IndexError as i:
            logging.warning(i)
            eimax = 1

        except Exception as e:
            logging.warning(e)

        self.prompt("(result_collection: {}, account_id: {}) Account's Post Types's Engagement Index Max value : {}".format(self.account_result_collection.name, accountID, str(eimax)))

        return eimax

    def getAccountPostTypeMinEIScore(self, accountID):

        self.prompt("(result_collection: {}, account_id: {}) Getting Account's Post Types's Engagement Index Min value".format(self.account_result_collection.name, accountID))
        listofscore = []

        try:
            latestresultdocument = self.account_result_collection.find(
                {
                    self.account_id_key: accountID
                }
            ).sort(
                'result_createdDate', -1
            ).limit(1)

            manytypes = list(latestresultdocument)[0]['post_type_result']

            for atype in manytypes:
                listofscore.append(manytypes[atype]['scores']['engagement_index_score'])

            [x for x in listofscore if x is not None]
            listofscore.sort()
            eimin = listofscore[0]

        except KeyError as k:
            logging.warning(k)
            eimin = 0

        except IndexError as i:
            logging.warning(i)
            eimin = 0

        except Exception as e:
            logging.warning(e)

        self.prompt("(result_collection: {}, account_id: {}) Account's Post Types's Engagement Index Min value : {}".format(self.account_result_collection.name, accountID, str(eimin)))

        return eimin

    def getPostTypeMaxEIScore(self):

        self.prompt("(result_collection: {}) Getting Post Types's Engagement Index Max value".format(self.account_result_collection.name))

        try:
            eimax = self.post_type_result_collection.find({
                'result.scores.engagement_index_score': {'$exists': True, '$ne': None}
            }).sort([
                # ['result_createdDate', -1],
                ['result.scores.engagement_index_score', -1]
            ]).limit(1)

            eimax = list(eimax)[0]['result']['scores']['engagement_index_score']

        except KeyError as k:
            logging.warning(k)
            eimax = 1

        except IndexError as i:
            logging.warning(i)
            eimax = 1

        except Exception as e:
            logging.warning(e)

        self.prompt("(result_collection: {}) Post Types's Engagement Index Max value : {}".format(self.account_result_collection.name, str(eimax)))

        return eimax

    def getPostTypeMinEIScore(self):

        self.prompt("(result_collection: {}) Getting Post Types's Engagement Index Min value".format(self.account_result_collection.name))

        try:
            eimin = self.post_type_result_collection.find({
                'result.scores.engagement_index_score': {'$exists': True, '$ne': None}
            }).sort([
                # ['result_createdDate', -1],
                ['result.scores.engagement_index_score', 1]
            ]).limit(1)

            eimin = list(eimin)[0]['result']['scores']['engagement_index_score']

        except KeyError as k:
            logging.warning(k)
            eimin = 0

        except IndexError as i:
            logging.warning(i)
            eimin = 0

        except Exception as e:
            logging.warning(e)

        self.prompt("(result_collection: {}) Post Types's Engagement Index Min value : {}".format(self.account_result_collection.name, str(eimin)))

        return eimin


class TwitterMongoConnector(MongoConnector):

    '''
        Subclass dari kelas MongoConnector() untuk twitter :

            * database:             DEFAULT: 'egovbench_sosmed'
            * account_collection:   'twitter_accounts'
            * post_collection:      'twitter_posts'
            * temp_collection:      'twitter_temp'
            * pemda_id_key:         DEFAULT: 'pemda_id'
            * account_type_key:     'account_type'
            * account_id_key:       'account_id'
            * post_id_key:          'tweet_id'
            * account_follower_key: 'account_followerCount'
    '''

    def __init__(self):
        super().__init__(
            'twitter_accounts',
            'twitter_posts',
            'twitter_comments',
            'twitter_temp',
            'twitter_accounts_result',
            'twitter_posts_types_result',
            'account_type',
            'account_id',
            'tweet_id',
            'tweet_type',
            'tweet_id',
            'account_followerCount',
            'twitter_resmi'
        )


class YoutubeMongoConnector(MongoConnector):

    '''
        Subclass dari kelas MongoConnector() untuk youtube :

            * database:             DEFAULT: 'egovbench_sosmed'
            * account_collection:   'youtube_accounts'
            * post_collection:      'youtube_posts'
            * temp_collection:      'youtube_temp'
            * pemda_id_key:         DEFAULT: 'pemda_id'
            * account_type_key:     'channel_type'
            * account_id_key:       'channel_id'
            * post_id_key:          'video_id'
            * account_follower_key: 'channel_subscriberCount'
    '''

    def __init__(self):
        super().__init__(
            'youtube_accounts',
            'youtube_posts',
            'youtube_comments',
            'youtube_temp',
            'youtube_accounts_result',
            'youtube_posts_types_result',
            'channel_type',
            'channel_id',
            'video_id',
            '',
            'comment_id',
            'channel_subscriberCount',
            'youtube_resmi'
        )

    def getMaxViewCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: youtube_accounts) Getting channel_viewCount max value . . .')

        try:
            collection_maxviewCount = self.account_result_collection.find({
                'result_createdDate': {'$ne': self.current_date_yyyymmdd},
                'result.statistics.viewCount': {'$exists': True, '$ne': None}
            }).sort(
                'result.statistics.viewCount', -1
            ).limit(1)

            collection_maxviewCount = list(collection_maxviewCount)[0]['result']['statistics']['viewCount']
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_collection: youtube_accounts) channel_viewCount max value: ' + str(collection_maxviewCount))

        return collection_maxviewCount

    def getMinViewCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: youtube_accounts) Getting channel_viewCount min value . . .')

        try:
            collection_minviewCount = self.account_result_collection.find({
                'result_createdDate': {'$ne': self.current_date_yyyymmdd},
                'result.statistics.viewCount': {'$exists': True, '$ne': None}
            }).sort(
                'result.statistics.viewCount', 1
            ).limit(1)

            collection_minviewCount = list(collection_minviewCount)[0]['result']['statistics']['viewCount']
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_collection: youtube_accounts) channel_viewCount min value: ' + str(collection_minviewCount))

        return collection_minviewCount


class FacebookMongoConnector(MongoConnector):

    '''
        Subclass dari kelas MongoConnector() untuk facebook :

            * database:             DEFAULT: 'egovbench_sosmed'
            * account_collection:   'facebook_accounts'
            * post_collection:      'facebook_posts'
            * temp_collection:      'facebook_temp'
            * pemda_id_key:         DEFAULT: 'pemda_id'
            * account_type_key:     'page_type'
            * account_id_key:       'page_id'
            * post_id_key:          'post_id'
            * account_follower_key: 'page_fanCount'
    '''

    def __init__(self):
        super().__init__(
            'facebook_accounts',
            'facebook_posts',
            'facebook_comments',
            'facebook_temp',
            'facebook_accounts_result',
            'facebook_posts_types_result',
            'page_type',
            'page_id',
            'post_id',
            'post_type',
            'comment_id',
            'page_fanCount',
            'youtube_resmi'
        )

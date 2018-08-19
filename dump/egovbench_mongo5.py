import pymongo
import logging
import json
import datetime


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
        database='egovbench3'
    ):
        '''
            Inisiasi :

            * database:             nama database di MongoDB (default: egovbench_sosmed)

            * account_collection:   collection yang terdiri dari dokumen milik akun
            * post_collection:      collection yang terdiri dari dokumen milik post
            * temp_collection:      collection yang berfungsi untuk menampung data yang dibaca oleh trigger
            * temp_collection.name: nama dari temp_collection yang merupakan bagian dari prosedur reset temp_collection

            * pemda_id_key:         key dari id pemda (default: pemda_id)
            * account_type_key:     key dari tipe akun (ex: channel_type)
            * account_id_key:       key dari id akun dari sosmed (ex: page_id)
            * post_id_key:          key dari id post dari sosmed (ex: tweet_id)

            * account_follower_key: key dari jumlah follower (ex: account_followerCount)

        '''

        client = pymongo.MongoClient()
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

    def prompt(self, texts):

        ''' Prompt dengan nama file untuk logging dengan level info agar mudah untuk di track'''

        logging.info('[EGOVBENCH_MONGO]>' + ' ' + texts)

    #                                       C R A W L E R
    # ------------------------------------------------------------------------------------------

    def collectAccounts(self):

        self.prompt('(accounts collection: {}) Collecting account from database . . .'.format(self.accounts_collection))

        accounts = self.accounts_collection.find({})

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

        '''
            Meng-update post pada post collection, dengan filter _id (yang merupakan atribut post_id_key dari postDocument).
            Bila filter tidak terpenuhi, akan menambah document baru ke dalam post collection.
        '''

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

        '''
            Meng-update post pada post collection, dengan filter _id (yang merupakan atribut post_id_key dari commentDocument).
            Bila filter tidak terpenuhi, akan menambah document baru ke dalam post collection.
        '''

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
            self.prompt('({}: {}) comment updated !'.format(self.comment_id_key, commentDocument['_id']))

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

        current_date_yyyymmdd = datetime.datetime.today().strftime('%Y-%m-%d')

        updateDocument['result_createdDate'] = "2018-05-29"

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

        self.prompt('({}: {}) Updating result . . .'.format(self.post_type_key, updateDocument[self.post_type_key]))

        current_date_yyyymmdd = datetime.datetime.today().strftime('%Y-%m-%d')

        updateDocument['result_createdDate'] = '2018-06-01'

        try:
            self.post_type_result_collection.update_many(
                {
                    self.post_type_key: updateDocument[self.post_type_key],
                    'result_createdDate': updateDocument['result_createdDate']
                },
                {
                    '$set': updateDocument
                },
                upsert=True
            )

            self.prompt('({}: {}) Result updated!'.format(self.post_type_key, updateDocument[self.post_type_key]))

        except Exception as e:
            logging.warning(e)

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

    def getMaxFollowerCountFromResult(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: {}) Getting {} max value . . .'.format(self.account_collection.name, self.account_follower_key))

        try:
            result_page_id_list = self.account_result_collection.distinct(self.account_id_key)

            collection_maxfollowerCount = self.account_collection.find({
                self.account_follower_key: {'$exists': True, '$ne': None},
                '_id': {'$in': result_page_id_list}
            }).sort(
                self.account_follower_key, -1
            ).limit(1)

            collection_maxfollowerCount = list(collection_maxfollowerCount)[0][self.account_follower_key]
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_collection: {}) {} max value: '.format(self.account_collection.name, self.account_follower_key) + str(collection_maxfollowerCount))

        return collection_maxfollowerCount

    def getMinFollowerCountFromResult(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: {}) Getting {} min value . . .'.format(self.account_collection.name, self.account_follower_key))

        try:
            result_page_id_list = self.account_result_collection.distinct(self.account_id_key)

            collection_minfollowerCount = self.account_collection.find({
                self.account_follower_key: {'$exists': True, '$ne': None},
                '_id': {'$in': result_page_id_list}
            }).sort(
                self.account_follower_key, 1
            ).limit(1)

            collection_minfollowerCount = list(collection_minfollowerCount)[0][self.account_follower_key]
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_collection: {}) {} min value: '.format(self.account_collection.name, self.account_follower_key) + str(collection_minfollowerCount))

        return collection_minfollowerCount

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
            'account_followerCount'
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
            'channel_subscriberCount'
        )

    def getMaxSubscriberCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: youtube_accounts) Getting channel_subscriberCount max value . . .')

        try:
            collection_maxsubscriberCount = self.account_collection.find({
                'channel_subscriberCount': {'$exists': True, '$ne': None}
            }).sort(
                'channel_subscriberCount', -1
            ).limit(1)

            collection_maxsubscriberCount = list(collection_maxsubscriberCount)[0]['channel_subscriberCount']
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_collection: youtube_accounts) channel_subscriberCount max value: ' + str(collection_maxsubscriberCount))

        return collection_maxsubscriberCount

    def getMinSubscriberCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: youtube_accounts) Getting channel_subscriberCount min value . . .')

        try:
            collection_minsubscriberCount = self.account_collection.find({
                'channel_subscriberCount': {'$exists': True, '$ne': None}
            }).sort(
                'channel_subscriberCount', 1
            ).limit(1)

            collection_minsubscriberCount = list(collection_minsubscriberCount)[0]['channel_subscriberCount']
        except Exception as e:
            logging.warning(e)

        self.prompt('(account_collection: youtube_accounts) channel_subscriberCount min value: ' + str(collection_minsubscriberCount))

        return collection_minsubscriberCount

    def getMaxViewCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: youtube_accounts) Getting channel_viewCount max value . . .')

        try:
            collection_maxviewCount = self.account_result_collection.find({
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
            'page_fanCount')

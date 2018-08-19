import pymongo
import logging


class MongoConnector():

    '''
        Kelas ini berfungsi untuk mengkomunikasikan MongoDB dengan sistem
    '''

    def __init__(
        self,
        accountCollection,
        postCollection,
        tempCollection,
        accountTypeKey,
        accountIDKey,
        postIDKey,
        accountFollowerKey,
        database='experimental'
    ):
        '''
            Inisiasi :

            * database:             nama database di MongoDB (default: egovbench_sosmed)

            * account_collection:   collection yang terdiri dari dokumen milik akun
            * post_collection:      collection yang terdiri dari dokumen milik post
            * temp_collection:      collection yang berfungsi untuk menampung data yang dibaca oleh trigger
            * temp_collection_name: nama dari temp_collection yang merupakan bagian dari prosedur reset temp_collection

            * pemda_id_key:         key dari id pemda (default: pemda_id)
            * account_type_key:     key dari tipe akun (ex: channel_type)
            * account_id_key:       key dari id akun dari sosmed (ex: page_id)
            * post_id_key:          key dari id post dari sosmed (ex: tweet_id)

            * account_follower_key: key dari jumlah follower (ex: account_followerCount)

        '''

        client = pymongo.MongoClient()
        self.database = client[database]

        self.account_collection = self.database[accountCollection]
        self.post_collection = self.database[postCollection]
        self.temp_collection = self.database[tempCollection]
        self.temp_collection_name = tempCollection

        self.pemda_id_key = 'pemda_id'
        self.account_type_key = accountTypeKey
        self.account_id_key = accountIDKey
        self.post_id_key = postIDKey

        self.account_follower_key = accountFollowerKey

    def prompt(self, texts):

        ''' Prompt dengan nama file untuk logging dengan level info agar mudah untuk di track'''

        logging.info('[EGOVBENCH_MONGO]>' + ' ' + texts)


    #                                       C R A W L E R
    # ------------------------------------------------------------------------------------------

    def checkAccount(self, pemda_id, account_id):

        '''
            Mengecek apakah pemda_id telah memiliki account_id di database.
        '''

        self.prompt('({}: {}) Checking account : {}'.format(self.pemda_id_key, pemda_id, account_id))

        pemda_id = int(pemda_id)
        account_id = account_id.lower()

        try:
            a = self.account_collection.find(
                {
                    self.pemda_id_key: pemda_id,
                }
            ).distinct(self.account_id_key)

            listakun = ', '.join(a)
            self.prompt('({}: {}) Existing account : {}'.format(self.pemda_id_key, pemda_id, listakun))

            account_exist = True if account_id in a else False
            self.prompt('({}: {}) Account status : {}'.format(self.account_id_key, account_id, 'Exist' if account_exist else 'Not Exist !'))

            return account_exist

        except Exception as e:
            logging.warning(e)

    def updatePost(self, postDocument):

        '''
            Meng-update post pada post collection, dengan filter _id (yang merupakan atribut post_id_key dari postDocument).
            Bila filter tidak terpenuhi, akan menambah document baru ke dalam post collection.
        '''

        self.prompt('({}: {}) Updating post please wait . . .'.format(self.post_id_key, postDocument[self.post_id_key]))

        try:
            self.post_collection.update_one(
                {
                    '_id': postDocument[self.post_id_key]
                },
                {
                    '$set': postDocument
                },
                upsert=True
            )
            self.prompt('({}: {}) Post updated !'.format(self.post_id_key, postDocument[self.post_id_key]))

        except Exception as e:
            logging.warning(e)

    def updateAccount(self, accountDocument):

        '''
            Meng-update post pada post collection, dengan filter pemda_id_key (dan account_type_key bila ada) dari accountDocument.
            Bila filter tidak terpenuhi, akan menambah document baru ke dalam post collection.
        '''

        updatefilter = {
            self.pemda_id_key: int(accountDocument[self.pemda_id_key]),
        }
        if self.account_type_key in accountDocument:
            updatefilter[self.account_type_key] = accountDocument[self.account_type_key]

        self.prompt('({}: {}, {}: {}) Updating account please wait . . .'.format(
            self.pemda_id_key,
            accountDocument[self.pemda_id_key],
            self.account_type_key if self.account_type_key in accountDocument else 'resmi',
            accountDocument[self.account_type_key] if self.account_type_key in accountDocument else 'resmi'
        ))

        try:
            self.account_collection.update_one(
                updatefilter,
                {
                    '$set': accountDocument
                },
                upsert=True
            )
            self.prompt('({}: {}, {}: {}) Account Updated !'.format(
                self.pemda_id_key,
                accountDocument[self.pemda_id_key],
                self.account_type_key if self.account_type_key in accountDocument else 'resmi',
                accountDocument[self.account_type_key] if self.account_type_key in accountDocument else 'resmi'
            ))

            self.insertTemp(accountDocument)

        except Exception as e:
            logging.warning(e)

    def insertTemp(self, document):

        '''
            Meng-insert document ke dalam temp collection untuk mengaktifkan trigger.
        '''

        self.prompt('({}: {}) Creating temp file for trigger . . .'.format(self.account_id_key, document[self.account_id_key]))

        try:
            self.temp_collection.insert_one(document)

            self.prompt('({}: {}) Temp file created!'.format(self.account_id_key, document[self.account_id_key]))

        except Exception as e:
            logging.warning(e)

    #                                       T R I G G E R
    # ------------------------------------------------------------------------------------------

    def resetTemp(self):

        '''
            Mereset isi temp collection
        '''

        self.prompt('(temp_collection: {}) Resetting temp collection . . .'.format(self.temp_collection_name))

        try:
            self.database.drop_collection(self.temp_collection_name)
            self.database.create_collection(self.temp_collection_name, capped=True, size=10000000)

            self.prompt('(temp_collection: {}) Temp collection resetted!'.format(self.temp_collection_name))

        except Exception as e:
            logging.warning(e)

    def activateTailableCursor(self):

        '''
            Mengaktifkan tailable cursor pada trigger.
            Tailable cursor mampu membaca input data yang baru saja masuk ke collection.
        '''

        self.prompt('(temp_collection: {}) Activating tailable cursor . . .'.format(self.temp_collection_name))

        try:
            cursor = self.temp_collection.find(cursor_type=pymongo.CursorType.TAILABLE_AWAIT)
            return cursor

        except Exception as e:
            logging.warning(e)

    def updateResult(self, updateDocument):

        '''
            Mengupdate score pada account collection
        '''

        self.prompt('({}: {}) Updating result . . .'.format(self.account_id_key, updateDocument[self.account_id_key]))

        try:
            self.account_collection.update_many(
                {
                    self.account_id_key: updateDocument[self.account_id_key]
                },
                {
                    '$set': updateDocument
                },
                upsert=True
            )

            self.prompt('({}: {}) Result updated!'.format(self.account_id_key, updateDocument[self.account_id_key]))

        except Exception as e:
            logging.warning(e)

    #                                       S C O R E R
    # ------------------------------------------------------------------------------------------

    def getFieldNotZeroCount(self, accountID, field):

        '''
            Mendapatkan jumlah post dengan nilai field tidak nol (ex: post memiliki paling tidak satu like)
            yang dimiliki oleh accountID.
        '''

        self.prompt('({}: {}) Getting total {}-not-zero . . .'.format(self.account_id_key, accountID, field))

        try:
            account_fieldnotzeroCount = self.post_collection.count({self.account_id_key: accountID, field: {'$ne': 0}})

        except Exception as e:
            logging.warning(e)
            account_fieldnotzeroCount = 0

        self.prompt('({}: {}) total {}-not-zero: {}'.format(self.account_id_key, accountID, field, str(account_fieldnotzeroCount)))

        return account_fieldnotzeroCount

    def getFieldCount(self, accountID, field):

        '''
            Mendapatkan jumlah total field yang dimiliki oleh accountID
        '''

        self.prompt('({}: {}) Getting total {} . . .'.format(self.account_id_key, accountID, field))

        try:
            account_fieldCount = self.post_collection.aggregate(
                [
                    {
                        '$match': {
                            self.account_id_key: accountID
                        }
                    },
                    {
                        '$group': {
                            '_id': '$%s' % (self.account_id_key),
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

        self.prompt('({}: {}) Total {}: {}'.format(self.account_id_key, accountID, field, str(account_fieldCount)))

        return account_fieldCount

    def getPostCount(self, accountID):

        '''
            Mendapatkan jumlah post yang dimiliki oleh accountID
        '''

        self.prompt('({}: {}) Getting total {}. . .'.format(self.account_id_key, accountID, self.post_id_key))

        try:
            account_postCount = self.post_collection.count({self.account_id_key: accountID})

        except Exception as e:
            logging.warning(e)
            account_postCount = 0

        self.prompt('({}: {}) Total {}: {}'.format(self.account_id_key, accountID, self.post_id_key, str(account_postCount)))

        return account_postCount

    def getFollowerCount(self, accountID):

        '''
            Mendapatkan jumlah follower yang dimiliki oleh accountID
        '''

        self.prompt('({}: {}) Getting {} . . .'.format(self.account_id_key, accountID, self.account_follower_key))

        try:
            account_followerCount = self.account_collection.find({self.account_id_key: accountID}, {self.account_follower_key: 1})
            account_followerCount = account_followerCount[0][self.account_follower_key]

        except Exception as e:
            logging.warning(e)
            account_followerCount = 0

        self.prompt('({}: {}) {}: {}'.format(self.account_id_key, accountID, self.account_follower_key, str(account_followerCount)))

        return account_followerCount


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
        super().__init__('twitter_accounts', 'twitter_posts', 'twitter_temp', 'account_type', 'account_id', 'tweet_id', 'account_followerCount')


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
        super().__init__('youtube_accounts', 'youtube_posts', 'youtube_temp', 'channel_type', 'channel_id', 'video_id', 'channel_subscriberCount')

    def getMaxSubscriberCount(self):

        '''
            Mendapatkan nilai maksimum dari field
        '''

        self.prompt('(account_collection: youtube_accounts) Getting channel_subscriberCount max value . . .')

        try:
            collection_maxsubscriberCount = self.account_collection.find({
                self.account_id_key: {'$ne': ""}
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
                self.account_id_key: {'$ne': ""}
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
            collection_maxviewCount = self.account_collection.find({
                'result': {'$exists': True, '$ne': None}
            }).sort(
                'result.channel_statistics.channel_viewCount', -1
            ).limit(1)

            collection_maxviewCount = list(collection_maxviewCount)[0]['result']['channel_statistics']['channel_viewCount']
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
            collection_minviewCount = self.account_collection.find({
                'result': {'$exists': True, '$ne': None}
            }).sort(
                'result.channel_statistics.channel_viewCount', 1
            ).limit(1)

            collection_minviewCount = list(collection_minviewCount)[0]['result']['channel_statistics']['channel_viewCount']
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
        super().__init__('facebook_accounts', 'facebook_posts', 'facebook_temp', 'page_type', 'page_id', 'post_id', 'page_fanCount')

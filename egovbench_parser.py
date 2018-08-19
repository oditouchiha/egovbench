# try:
#     import simplejson as json
# except ImportError:
#     import json
import logging


class Parser():

    '''
        Kelas ini berfungsi untuk mem-parsing hasil crawling ke dalam struktur document yang disimpan di database MongoDB.
    '''

    def __init__(self, accountTypeKey, accountIDKey, postIDKey, commentIDKey):

        '''
            Inisiasi :

            * account_type_key: key dari tipe akun dari sosmed (ex: channel_type)
            * account_id_key:   key dari id akun dari sosmed (ex: page_id)
            * post_id_key:      key dari id post dari sosmed (ex: tweet_id)

        '''

        self.account_type_key = accountTypeKey
        self.account_id_key = accountIDKey
        self.post_id_key = postIDKey
        self.comment_id_key = commentIDKey

    def prompt(self, texts):

        ''' Prompt dengan nama file untuk logging dengan level info agar mudah untuk di track'''

        logging.info('[EGOVBENCH_PARSER]>' + ' ' + texts)

    def getPostDocument(self, complete_dict):

        '''
            Fungsi ini memparsing hasil crawling ke dalam bentuk json/dict sebagai berikut:

            {
                '_id': <ID unik dari post (berasal dari post_id_key)>
                account_id_key: <ID dari akun>
                .
                .
                . Isi atribut ['post'] dari hasil crawling (complete_dict)
                .
            }

        '''

        postdocument = complete_dict['post']

        postdocument['_id'] = postdocument[self.post_id_key]
        postdocument.pop(self.post_id_key, None)

        postdocument[self.account_id_key] = complete_dict['account'][self.account_id_key].lower()

        self.prompt('({}: {} ({}: {})) Post document created!'.format(
            self.account_id_key,
            postdocument[self.account_id_key],
            self.post_id_key,
            postdocument['_id']
        ))

        return postdocument

    def getCommentDocument(self, complete_dict):

        '''
            Fungsi ini memparsing hasil crawling ke dalam bentuk json/dict sebagai berikut:

            {
                '_id': <ID unik dari comment (berasal dari comment_id_key)>
                account_id_key: <ID dari akun>
                .
                .
                . Isi atribut ['comment'] dari hasil crawling (complete_dict)
                .
            }

        '''

        commentdocument = complete_dict['comment']

        for comment in commentdocument:

            comment['_id'] = comment[self.comment_id_key]
            comment.pop(self.comment_id_key, None)

            comment[self.account_id_key] = complete_dict['account'][self.account_id_key].lower()

            self.prompt('({}: {} ({}: {})) Comment document created!'.format(
                self.account_id_key,
                comment[self.account_id_key],
                self.comment_id_key,
                comment['_id']
            ))

        return commentdocument

    def getAccountDocument(self, complete_dict):

        '''
            Fungsi ini memparsing hasil crawling ke dalam bentuk json/dict sebagai berikut:

            {
                'pemda_id': <ID pemda>
                'pemda_name': <Nama pemda>
                .
                .
                . Isi atribut ['account'] dari hasil crawling (complete_dict)
                .
            }

        '''

        accountdocument = complete_dict['account']

        accountdocument['_id'] = complete_dict['account'][self.account_id_key].lower()
        accountdocument.pop(self.account_id_key, None)

        self.prompt('({}: {}) Account document created!'.format(
            self.account_id_key,
            accountdocument['_id']
        ))

        return accountdocument


class TwitterParser(Parser):

    '''
        Subclass dari kelas Parser() untuk twitter :

            * account_type_key: account_type
            * account_id_key:   account_id
            * post_id_key:      tweet_id
    '''

    def __init__(self):
        super().__init__('account_type', 'account_id', 'tweet_id', 'reply_id')


class YoutubeParser(Parser):

    '''
        Subclass dari kelas Parser() untuk youtube :

            * account_type_key: channel_type
            * account_id_key:   channel_id
            * post_id_key:      video_id
    '''

    def __init__(self):
        super().__init__('channel_type', 'channel_id', 'video_id', 'comment_id')


class FacebookParser(Parser):

    '''
        Subclass dari kelas Parser() untuk facebook :

            * account_type_key: page_type
            * account_id_key:   page_id
            * post_id_key:      post_id
    '''

    def __init__(self):
        super().__init__('page_type', 'page_id', 'post_id', 'comment_id')

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint
import datetime
import logging

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

sheet = client.open('situs.csv').sheet1
pp = pprint.PrettyPrinter()


class Collector():

    def __init__(self, pemdaaccount_column, influenceraccount_column):
        self.pemdaaccount_column = pemdaaccount_column
        self.influenceraccount_column = influenceraccount_column

    def prompt(self, texts):
        logging.info('[EGOVBENCH_GPSREADSHEET]>' + ' ' + texts)

    def getPemdaIDList(self):
        self.prompt('(column: {}) Retrieving data from spreadsheets . . .'.format(sheet.col_values(3)[0]))
        return sheet.col_values(3)[1:]

    def getPemdaNameList(self):
        self.prompt('(column: {}) Retrieving data from spreadsheets . . .'.format(sheet.col_values(1)[0]))
        return sheet.col_values(1)[1:]

    def getPemdaAccountList(self):
        self.prompt('(column: {}) Retrieving data from spreadsheets . . .'.format(sheet.col_values(self.pemdaaccount_column)[0]))
        return sheet.col_values(self.pemdaaccount_column)[1:]

    def getInfluencerAccountList(self):
        self.prompt('(column: {}) Retrieving data from spreadsheets . . .'.format(sheet.col_values(self.influenceraccount_column)[0]))
        return sheet.col_values(self.influenceraccount_column)[1:]


class YoutubeCollector(Collector):

    def __init__(self):
        super().__init__(9, 12)


class TwitterCollector(Collector):

    def __init__(self):
        super().__init__(8, 11)


class FacebookCollector(Collector):

    def __init__(self):
        super().__init__(7, 10)

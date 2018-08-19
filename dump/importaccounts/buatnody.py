from egovbench_gspreadsheet import YoutubeCollector, FacebookCollector, TwitterCollector, Collector
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint
import pymongo


c = Collector(None, None)

pemdaIDlist = c.getPemdaIDList()
pemdaNamelist = c.getPemdaNameList()

yc = YoutubeCollector()
tc = TwitterCollector()
fc = FacebookCollector()

ycrlist = yc.getPemdaAccountList()
ycilist = yc.getInfluencerAccountList()

tcrlist = tc.getPemdaAccountList()
tcilist = tc.getInfluencerAccountList()

fcrlist = fc.getPemdaAccountList()
fcilist = fc.getInfluencerAccountList()

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

sheet = client.open('situs.csv').sheet1
pp = pprint.PrettyPrinter()

tcrnlist = sheet.col_values(13)[1:]


for pemdaID in pemdaIDlist:

    pemdaName = pemdaNamelist[pemdaIDlist.index(pemdaID)]
    ycr = ycrlist[pemdaIDlist.index(pemdaID)].lower()
    yci = ycilist[pemdaIDlist.index(pemdaID)].lower()
    tcr = tcrlist[pemdaIDlist.index(pemdaID)].lower()
    tci = tcilist[pemdaIDlist.index(pemdaID)].lower()
    fcr = fcrlist[pemdaIDlist.index(pemdaID)].lower()
    fci = fcilist[pemdaIDlist.index(pemdaID)].lower()
    tcrn = tcrnlist[pemdaIDlist.index(pemdaID)]

    jeson = {}
    jeson['_id'] = int(pemdaID)
    jeson['name'] = pemdaName
    jeson['youtube-resmi'] = ycr
    jeson['youtube-influencer'] = yci
    jeson['twitter-resmi'] = tcr
    jeson['twitter-resmi-number'] = tcrn
    jeson['twitter-influencer'] = tci
    jeson['facebook-resmi'] = fcr
    jeson['facebook-influencer'] = fci
    jeson['used'] = False

    print(jeson)

    client = pymongo.MongoClient()
    database = client['egovbench-test']
    collection = database['listpemda']

    collection.insert(jeson)

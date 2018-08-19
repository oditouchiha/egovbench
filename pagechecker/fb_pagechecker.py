import time
# try:
from urllib.request import urlopen, Request
import spreadsheet

access_token = 'EAACDwR6nEuQBAKwTlfoHYP9bJkaZCukEjwKs2gNA0nEKusaqRl7IqjjYJna7TebhdgVF8KdMLCeURpU64gfhwEoV7LLDab3lKi5IbZAZBhny9GtFAXxdQob39URSM7GzogEEzKxykMfXJQ9EGl1dcINwdHZCAOkZD'


def request_until_succeed(url):
    req = Request(url)
    try:
        response = urlopen(req)
        if response.getcode() == 200:
            print('OK')
    except Exception as e:
        print("ERROR !!!")
        print(e)
        time.sleep(1)

        pass


def page_checker(pagename):
    base = "https://graph.facebook.com/v2.12"
    node = "/{}".format(pagename)
    parameters = "?access_token={}&fields=id,name".format(access_token)

    url = base + node + parameters

    request_until_succeed(url)


if __name__ == '__main__':
    idpagepemda = spreadsheet.scrapingcolumn(7, 1, -1)
    namapemda = spreadsheet.scrapingcolumn(1, 1, None)
    idpemda = spreadsheet.scrapingcolumn(3, 1, None)

    for satuanidpemda in idpemda:

        satuanidpagepemda = idpagepemda[idpemda.index(satuanidpemda)]

        if satuanidpagepemda is not '':

            print('Checking pemda No. {}: {} | FB page: {} . . .'.format(satuanidpemda, namapemda[idpemda.index(satuanidpemda)], satuanidpagepemda), end='', flush=True)
            page_checker(satuanidpagepemda)

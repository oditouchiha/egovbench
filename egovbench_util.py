import datetime
import logging


def cleanStrings(text):
    try:
        cleaned = text.encode('utf-8', 'ignore').decode('utf-8', 'ignore')
    except UnicodeDecodeError:
        cleaned = text.encode('utf-16', 'ignore').decode('utf-16', 'ignore')

    return cleaned


def formatTime(time, timeformat):
    returned_time = datetime.datetime.strptime(time, timeformat)
    returned_time = returned_time + datetime.timedelta(hours=+7)  # Waktu Indonesia
    returned_time = returned_time.strftime('%Y-%m-%d')

    return returned_time


def formatFacebookTime(time):
    return formatTime(time, '%Y-%m-%dT%H:%M:%S+0000')


def formatTwitterTime(time):
    return formatTime(time, '%a %b %d %H:%M:%S +0000 %Y')


def formatYoutubeTime(time):
    return formatTime(time, "%Y-%m-%dT%H:%M:%S.000Z")

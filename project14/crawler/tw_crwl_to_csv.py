# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from afinn import Afinn
import config
import json
import time
import csv
import sys


class CSVListener(StreamListener):

    def __init__(self, api=None):
        self.api = api
        self.filename = sys.argv[1] + '_' + \
            time.strftime('%Y%m%d-%H%M%S') + '.csv'
        csvFile = open(self.filename, 'w')
        csvWriter = csv.writer(csvFile)
        csvWriter.writerow(['text',
                            'created_at',
                            'user.location',
                            'user.id',
                            'user.name',
                            'senti_val'])

    def on_status(self, status):
        csvFile = open(self.filename, 'a')
        csvWriter = csv.writer(csvFile)

        if not 'RT @' in status.text:
            try:
                csvWriter.writerow([status.text,
                                    status.created_at,
                                    status.geo,
                                    status.user.location,
                                    status.user.id,
                                    status.user.name,
                                    afinn.score(status.text)])
            except Exception as e:
                print(e)
                pass

        csvFile.close()
        return

    def on_error(self, status_code):
        print('Encountered error with status code:', status_code)
        if status_code == 401:
            return False

    def on_delete(self, status_id, user_id):
        print("Delete notice")
        return

    def on_limit(self, track):
        print(time.strftime('%H:%M:%S'), "Rate limited, continuing")
        return True

    def on_timeout(self):
        print(sys.stderr, 'Timeout...')
        time.sleep(10)
        return

if __name__ == '__main__':

    auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_SECRET)
    stream = Stream(auth, CSVListener())

    afinn = Afinn()

    while True:
        try:
            stream.filter(track=sys.argv[2:])
        except:
            time.sleep(60 * 10)
            continue
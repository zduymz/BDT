from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from afinn import Afinn
import pykafka
import tweepy
import config
import json
import time
import sys


class TweetListener(StreamListener):

    def __init__(self):
        self.api=None
        self.client = pykafka.KafkaClient(config.KAKFA_BOOTSTRAP_SERVER)
        self.producer = self.client.topics['twitter'].get_producer()

    def on_status(self, status):
        send_data = '{}'
        if not 'RT @' in status.text:
            try:
                json_send_data = json.loads(send_data)

                json_send_data['id'] = status.id_str
                json_send_data['name'] = status.user.screen_name
                json_send_data['location'] = status.user.location
                json_send_data['text'] = status.text
                json_send_data['senti_val'] = afinn.score(status.text)

                self.producer.produce(bytes(json.dumps(json_send_data)))
            except Exception as e:
                print(e)
                pass

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



if __name__ == "__main__":

    auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_SECRET)

    afinn = Afinn()

    twitter_stream = Stream(auth, TweetListener())
    while True:
        try:
            twitter_stream.filter(languages=['en'], track=sys.argv[1:])
        except:
            time.sleep(60 * 10)
            continue

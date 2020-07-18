from TwitterAPI import TwitterAPI, TwitterConnectionError, TwitterRequestError
import time
import pandas as pd

class Frequency:
    """Track tweet download statistics"""

    def __init__(self):
        self.interval = 5  # seconds
        self.total_count = 0
        self.total_start = time.time()
        self.interval_count = 0
        self.interval_start = self.total_start
    def get_total_count(self):
        return int(self.total_count)

    def is_printing_results(self):
        now = time.time()
        elapsed = now - self.interval_start
        return elapsed >= self.interval

    def update(self):
        self.interval_count += 1
        self.total_count += 1
        now = time.time()
        elapsed = now - self.interval_start
        if elapsed >= self.interval:
            # timestamp : tps : total cumulative tweets : average tps
            print('%s -- %d\t%d\t%d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)),
                                        int(self.interval_count / elapsed),
                                        self.total_count,
                                        int(self.total_count / (now - self.total_start))))
            self.interval_start = now
            self.interval_count = 0

# Twitter API credentials
consumer_key = open('your_consumer_key.txt', 'r')
consumer_key = consumer_key.read()

consumer_secret = open('your_consumer_secret.txt','r')
consumer_secret = consumer_secret.read()

access_token = open('your_access_token.txt','r')
access_token = access_token.read()

access_token_secret = open('your_access_token_secret.txt','r')
access_token_secret = access_token_secret.read()
api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

freq = Frequency()
tweets_df = pd.DataFrame(columns=['created_at', 'text', 'id'])

# HOW TO HANDLE ALL TYPES OF STREAMING ERRORS.
# A DROPPED CONNECTION IS RE-ESTABLISHED.
total_count = freq.get_total_count()
while len(tweets_df) < 150000:
    try:
        r = api.request('statuses/sample')
        for item in r:
            if 'text' in item:
                freq.update()
                # take only tweets in English
                if item['lang'] == 'en':
                    tweets_df = tweets_df.append({'created_at':item['created_at'], 'text':item['text'], 'id':item['id']}, ignore_index=True)
                    if freq.is_printing_results():
                        print("total length of the dataframe" + str(len(tweets_df)))
            elif 'limit' in item:
                print('TWEETS SKIPPED: %s' % item['limit']['track'])
            elif 'warning' in item:
                print(item['warning'])
            elif 'disconnect' in item:
                event = item['disconnect']
                if event['code'] in [2, 5, 6, 7]:
                    # streaming connection rejected
                    raise Exception(event)
                print('RE-CONNECTING: %s' % event)
                break
        total_count = freq.get_total_count()
        print("total count " + str(total_count))
    except TwitterRequestError as e:
        if e.status_code < 500:
            print('REQUEST FAILED: %s' % e)
            break
    except TwitterConnectionError:
        pass
    except KeyboardInterrupt:
        print('TERMINATED BY USER')
        break
    except Exception as e:
        print('STOPPED: %s %s' % (type(e), e))
        break

tweets_df.to_csv('your_path\\tweets_sample_en.csv', sep='\t')
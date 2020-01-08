import tweepy
import psycopg2
from bs4 import BeautifulSoup
from collections import OrderedDict
import sqlite3
from preproc import *
from tqdm import tqdm
import csv
from threading import Thread
from queue import Queue

def init_twitter_API(dct):
    consumer_key = dct['consumer_key']
    consumer_secret = dct['consumer_secret']
    access_token = dct['access_token']
    access_token_secret = dct['access_token_secret']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.auth.API(auth,wait_on_rate_limit=True)
    return api
def insert_into_db(item):
        keys = item.keys()
        values = [item[x] for x in keys]
        try:
            cur = self.conn.cursor()
            cur.execute('insert into tweet_urls_crawled(%s) values %s;',(psycopg2.extensions.AsIs(','.join(keys)), tuple(values)))
            cur.close()
        except Exception as e:
            logger.debug("Postgres Failed "+str(e))
            return False
        return True
def crawl_twitter(curr_id,api,conn):
    try:
        for post in tweepy.Cursor(api.user_timeline,id=curr_id,summary=False,tweet_mode="extended").items():
            dc = OrderedDict()
            curr_post = post._json
            dc['tweet_from'] = curr_id
            dc['created_at'] = curr_post['created_at']
            ent_status_dct = curr_post.get("entities",False)
            if ent_status_dct:
                dc['hashtags'] = [x['text'] for x in curr_post['entities']['hashtags']]
                dc['urls'] = [x['expanded_url'] for x in curr_post['entities']['urls'] ]
                dc['user_mentions_id'] = [x['id'] for x in curr_post['entities']['user_mentions']]
                if 'media' in ent_status_dct:
                    dc['media'] = [x['media_url_https'] for x in curr_post['entities']['media']]
                dc['user_mentions_name'] = [x['screen_name'] for x in curr_post['entities']['user_mentions']]
            dc['origin_device'] = BeautifulSoup(curr_post['source'], 'html.parser').a.string
            dc['favorite_count'] = curr_post['favorite_count']
            dc['text'] = curr_post['full_text']
            dc['id'] = curr_post['id']
            dc['in_reply_to_screen_name']  = curr_post['in_reply_to_screen_name']
            dc['in_reply_to_user_id']  = curr_post['in_reply_to_user_id']
            dc['in_reply_to_status_id']  = curr_post['in_reply_to_status_id']
            dc['retweet_count']  = curr_post['retweet_count']
            rt_status_dct = curr_post.get('retweeted_status',False)
    #         adding retweet information because it is important.
            if rt_status_dct:
                dc['retweeted_status_text'] = curr_post['retweeted_status']['full_text']
                dc['retweeted_status_url'] = [x['expanded_url'] for x in curr_post['retweeted_status']['entities']['urls'] ]
                dc['retweeted_status_id'] = curr_post['retweeted_status']['id']
                dc['retweeted_status_user_name'] = curr_post['retweeted_status']['user']['name']
                dc['retweeted_status_user_handle'] = curr_post['retweeted_status']['user']['screen_name']
            insert_into_postgres(dc,conn)
    except Exception as e:
        print(str(e))

ls = pickle.load(open('pickles/tweet_article27519.pkl','rb'))
with pg_get_conn() as conn:
    cur = conn.cursor()
    cur.execute('Select distinct(tweet_from) from tweet_articles_tweepy');
    ans = cur.fetchall()
ans = [x[0] for x in ans]
new_ls = list(set(ls) - set(ans))

access_dct=[]
with open('files/twitteraccesscodes.csv','r') as f:
    cr = csv.reader(f)
    keys = next(cr)
    for row in cr:
        dct={}
        for i in range(len(row)):
            dct[keys[i]] = row[i]
        access_dct.append(dct)
q = Queue()
_ = [q.put(x) for x in new_ls]

def process(q,api,conn):
    while True:
        curr_id = q.get()
        crawl_twitter(curr_id,api,conn)
        q.task_done()

with pg_get_conn() as conn:
    conn.autocommit = True
    for dct in access_dct:
        api=init_twitter_API(dct)
        worker=Thread(target=process,args=(q,api,conn))
        # worker.setDaemon(True)
        worker.start()

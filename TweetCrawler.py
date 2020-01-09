import argparse
import csv
import getpass
import sys
from queue import Queue
from threading import Thread

from preproc import *


# import psycopg2


def get_credentials(authfile):
    try:
        credentials_list = []
        with open(authfile) as f:
            cr = csv.reader(f)
            keys = next(cr)
            for row in cr:
                dct = {}
                for i in range(len(row)):
                    dct[keys[i]] = row[i]
                credentials_list.append(dct)
        return credentials_list
    except Exception as e:
        logging.error("Cannot read credentials from file")
        sys.exit("Exiting Fatal Error")


def pg_get_conn(database="abhishek", user="abhishek", password=""):
    """Get Postgres connection for fakenews

    Returns:
        Connection object : returns Post gres connection object

    Args:
        database (str, optional): Name of database
        user (str, optional): Name of User
        password (str, optional): Password of user
    """
    try:
        conn = psycopg2.connect(database="fakenews",
                                user="fakenews", password="fnd", host='localhost', port='5432')
        return conn
    except Exception as e:
        logging.error("Problem Authenticating PostGre connection: " + str(e))


def init_twitter_API(dct):
    try:
        consumer_key = dct['consumer_key']
        consumer_secret = dct['consumer_secret']
        access_token = dct['access_token']
        access_token_secret = dct['access_token_secret']
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.auth.API(auth, wait_on_rate_limit=True)
    except Exception as e:
        logging.error("Problem Authenticating twitter credentials: " + str(e))
    return api


def insert_into_db(item, conn):
    keys = item.keys()
    values = [item[x] for x in keys]
    try:
        cur = conn.cursor()
        cur.execute('insert into tweet_urls_crawled(%s) values %s;',
                    (psycopg2.extensions.AsIs(','.join(keys)), tuple(values)))
        cur.close()
    except Exception as e:
        logging.error("Inserting into Database Failed: " + str(e))
        return False
    return True


def crawl_twitter(curr_id, api, conn):
    try:
        for post in tweepy.Cursor(api.user_timeline, id=curr_id, summary=False, tweet_mode="extended").items():
            dc = OrderedDict()
            curr_post = post._json
            dc['tweet_from'] = curr_id
            dc['created_at'] = curr_post['created_at']
            ent_status_dct = curr_post.get("entities", False)
            if ent_status_dct:
                dc['hashtags'] = [x['text'] for x in curr_post['entities']['hashtags']]
                dc['urls'] = [x['expanded_url'] for x in curr_post['entities']['urls']]
                dc['user_mentions_id'] = [x['id'] for x in curr_post['entities']['user_mentions']]
                if 'media' in ent_status_dct:
                    dc['media'] = [x['media_url_https'] for x in curr_post['entities']['media']]
                dc['user_mentions_name'] = [x['screen_name'] for x in curr_post['entities']['user_mentions']]
            dc['origin_device'] = BeautifulSoup(curr_post['source'], 'html.parser').a.string
            dc['favorite_count'] = curr_post['favorite_count']
            dc['text'] = curr_post['full_text']
            dc['id'] = curr_post['id']
            dc['in_reply_to_screen_name'] = curr_post['in_reply_to_screen_name']
            dc['in_reply_to_user_id'] = curr_post['in_reply_to_user_id']
            dc['in_reply_to_status_id'] = curr_post['in_reply_to_status_id']
            dc['retweet_count'] = curr_post['retweet_count']
            rt_status_dct = curr_post.get('retweeted_status', False)
            #         adding retweet information because it is important.
            if rt_status_dct:
                dc['retweeted_status_text'] = curr_post['retweeted_status']['full_text']
                dc['retweeted_status_url'] = [x['expanded_url'] for x in
                                              curr_post['retweeted_status']['entities']['urls']]
                dc['retweeted_status_id'] = curr_post['retweeted_status']['id']
                dc['retweeted_status_user_name'] = curr_post['retweeted_status']['user']['name']
                dc['retweeted_status_user_handle'] = curr_post['retweeted_status']['user']['screen_name']
            insert_into_postgres(dc, conn)
    except Exception as e:
        logging.info("Can't crawl ID " + curr_id + " exception: " + str(e))


def process(q, api, conn):
    while True:
        curr_id = q.get()
        crawl_twitter(curr_id, api, conn)
        q.task_done()


q = Queue()
_ = [q.put(x) for x in new_ls]

with pg_get_conn() as conn:
    conn.autocommit = True
    for dct in access_dct:
        api = init_twitter_API(dct)
        worker = Thread(target=process, args=(q, api, conn))
        # worker.setDaemon(True)
        worker.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Threaded crawler for crawling Twitter")
    parser.add_argument("--authcsv", default=None,
                        help="The path to the csv file cointaining authorization tokens for twitter")
    parser.add_argument("--dbname", default=None, help="Postgres database in which to enter the crawled data")
    parser.add_argument("--dbuser", default=None, help="User name for the Postgres database ")
    parser.add_argument("--outputcsv", default="crawled.csv", help="Output CSV file in which to dump crawled data")
    parser.add_argument("--threads", default=1, help="Number of threads to use for crawling")
    args = parser.parse_args()
    if bool(args.dbname) ^ bool(args.dbuser):  # check if only one of db parameter is set. Used XOR
        parser.error("Both --dbname and --dbuser should be set, you've set only one of them")
    if args.dbname:
        pg_password = getpass.getpass(prompt="Please enter password for the user of the postgres database ")
    if not parser.authcsv:
        auth_dct = {'consumer_key': getpass.getpass(prompt="Enter the consumer key obtained from twitter"),
                    'consumer_secret': getpass.getpass(prompt="Enter the consumer secret for twitter API"),
                    'access_token': getpass.getpass(prompt="Enter the access token for twitter API"),
                    'access_token_secret': getpass.getpass(prompt="Enter the access token secret for twitter API")}
        credentials = [auth_dct]
    else:
        credentials = get_credentials(parser.authcsv)

# TODO: use pandas to accumulate the crawled data and export easily as csv file
# TODO: reading from csv files for auth credentials can also be optimized using pandas
# TODO: check for robust handling of in memory data. Can be a problem in case of large crawls.
# TODO: Provide a sample systemd service file configuration

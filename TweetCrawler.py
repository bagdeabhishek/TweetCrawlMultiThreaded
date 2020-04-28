import argparse
import configparser
import csv
import getpass
import logging
import os
import sys
from queue import Queue
from threading import Thread

import pandas as pd
import psycopg2
import tweepy
from bs4 import BeautifulSoup

INDIA_ID_YAHOO = "23424848"
PICKLE_FILE_CRAWLED_DATA = "./crawled.txt"


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
    except FileNotFoundError as e:
        logging.error("Cannot read credentials from file: " + str(e))
        sys.exit("Exiting Fatal Error")


def pg_get_conn(database, user, password, host, port):
    """Get Postgres connection for fakenews

    Returns:
        Connection object : returns Post gres connection object

    Args:
        database (str, optional): Name of database
        user (str, optional): Name of User
        password (str, optional): Password of user
        :param host:
        :type host:
    """
    try:
        conn = psycopg2.connect(database=database,
                                user=user, password=password, host=host, port=port)
        conn.autocommit = True
        return conn
    except psycopg2.DatabaseError as e:
        logging.error("Problem Connecting to database:  " + str(e))


def insert_into_postgres(posts, conn, tablename):
    for item in posts:
        keys = list(item.keys())
        values = [item[x] for x in keys]
        try:
            cur = conn.cursor()
            cur.execute('insert into {}(%s) values %s;'.format(tablename),
                        (psycopg2.extensions.AsIs(','.join(keys)), tuple(values)))
            cur.close()
        except psycopg2.DatabaseError as e:
            logging.critical("Insert Failed " + str(e))
    return


def init_twitterAPI(dct):
    consumer_key = dct['consumer_key']
    consumer_secret = dct['consumer_secret']
    access_token = dct['access_token']
    access_token_secret = dct['access_token_secret']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.auth.API(auth, wait_on_rate_limit=True)
    return api


def mark_handle_crawled(curr_id):
    with open(PICKLE_FILE_CRAWLED_DATA, 'a') as f:
        f.write(curr_id + '\n')
    return


def crawl_twitter(q, api, conn, output_folder, tablename, search=False):
    try:
        while not q.empty():
            curr_id = q.get()
            posts = []
            logging.info("Crawling handle " + curr_id)
            if search:
                cursor = tweepy.Cursor(api.search, q=curr_id, summary=False, tweet_mode="extended",
                                       count=100).items()
            else:
                cursor = tweepy.Cursor(api.user_timeline, id=curr_id, summary=False, tweet_mode="extended").items()
            try:
                for post in cursor:
                    dc = {}
                    curr_post = post._json
                    dc['tweet_from'] = curr_post['user']['screen_name']
                    dc['created_at'] = curr_post['created_at']
                    ent_status_dct = curr_post.get("entities", False)
                    if ent_status_dct:
                        dc['hashtags'] = [x['text'] for x in curr_post['entities']['hashtags']]
                        dc['urls'] = [x['expanded_url'] for x in curr_post['entities']['urls']]
                        dc['user_mentions_id'] = [x['id'] for x in curr_post['entities']['user_mentions']]
                        if 'media' in ent_status_dct:
                            dc['media'] = [x['media_url_https'] for x in curr_post['entities']['media']]
                        dc['user_mentions_name'] = [x['screen_name'] for x in curr_post['entities']['user_mentions']]
                    origin_raw_html = BeautifulSoup(curr_post['source'], 'html.parser').a
                    dc['origin_device'] = origin_raw_html.string if origin_raw_html else None
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
                    posts.append(dc)
            except tweepy.error.TweepError as e:
                logging.error("Can't crawl tweet, possibly parser error: " + str(curr_id) + " exception: " + str(e))
            if conn:
                insert_into_postgres(posts, conn, tablename)
            else:
                if not os.path.exists(output_folder):
                    os.mkdir(output_folder)
                csv_file = os.path.join(output_folder, curr_id + ".csv")
                df = pd.DataFrame(posts)
                df.to_csv(csv_file)
            mark_handle_crawled(curr_id)
            q.task_done()
    except tweepy.error.TweepError as e:
        logging.error("Can't crawl ID, error in Cursor" + str(curr_id) + " exception: " + str(e))
    return


def get_queue(file):
    try:
        q = Queue()
        with open(file) as f:
            for line in f:
                q.put(line)
        return q
    except FileNotFoundError as e:
        logging.error("Problem reading handles file: " + str(e))
        sys.exit("Exiting Fatal Error")


def init_crawler(no_of_threads, auth_list, db_credentials, handles_file, target_folder, trending):
    q = get_queue(handles_file) if not trending else get_trending_handles(auth_list)
    if db_credentials:
        conn = pg_get_conn(db_credentials["dbname"], db_credentials["dbuser"],
                           db_credentials["dbpass"], db_credentials["dbhost"],
                           db_credentials["dbport"])
    else:
        conn = None
    for i in range(int(no_of_threads)):
        api = init_twitterAPI(auth_list[i % len(auth_list)])
        worker = Thread(target=crawl_twitter, args=(q, api, conn, target_folder, db_credentials['tablename'], trending))
        worker.start()
    return


def get_user_input(input_type):
    if type == "twitterauth":
        auth_dct = {'consumer_key': getpass.getpass(prompt="Enter the consumer key obtained from twitter"),
                    'consumer_secret': getpass.getpass(prompt="Enter the consumer secret for twitter API"),
                    'access_token': getpass.getpass(prompt="Enter the access token for twitter API"),
                    'access_token_secret': getpass.getpass(prompt="Enter the access token secret for twitter API")}
        return auth_dct


def get_conf_file():
    configuration = {}
    config = configparser.ConfigParser()
    config.read('./tweet.ini')
    sections = config.sections()
    if 'Database' in sections:
        section = "Database"
        configuration['db_credentials'] = {'dbname': config.get(section, 'dbname'),
                                           'dbuser': config.get(section, 'dbuser'),
                                           'dbpass': config.get(section, 'dbpass'),
                                           'dbhost': config.get(section, 'dbhost'),
                                           'dbport': config.get(section, 'dbport'),
                                           'tablename': config.get(section, 'tablename')}
    else:
        configuration['db_credentials'] = None
    if 'Twitter' in sections:
        section = "Twitter"
        auth_file = config.get(section, 'authCSV')
        configuration['authcsv'] = get_credentials(auth_file) if auth_file else get_user_input('twitterauth')
        configuration['handles'] = config.get(section, "handlesFile")
    else:
        logging.error("Config file missing twitter section")
        sys.exit("Critical Error")
    if 'System' in sections:
        section = "System"
        configuration["threads"] = config.get(section, "Threads")
        configuration["target_folder"] = config.get(section, "CSVFolder")
    return configuration


def get_next_level_handles(database, user, password, host, port, rt=True):
    with pg_get_conn(database, user, password, host, port) as conn:
        cur = conn.cursor()
        if rt:
            cur.execute("""Select retweeted_status_user_handle from tweet_articles_tweepy""")
        else:
            cur.execute("""Select * from tweet_articles_tweepy""")
        ans = cur.fetchall()
    lse = set(x[0].replace("{", "").replace("}", "") for x in ans) if not rt else set(x[0] for x in ans)
    ans_handles = []
    if not rt:
        for x in lse:
            for y in x.split(","):
                if y != '':
                    ans_handles.append(y)
    else:
        ans_handles = list(lse)
    return ans_handles


def write_next_handles(new_handles, path_old_file):
    old_handles = set()
    with open(path_old_file) as o_handles:
        for o_handle in o_handles:
            if o_handle.startswith('@'):
                o_handle = o_handle.replace('@', '')
            old_handles.add(o_handle)
    new_handles = set(new_handles)
    next_level_handles = new_handles.difference(old_handles)
    with open(path_old_file + ".bk", 'a') as f:
        for item in old_handles:
            f.write("%s\n" % item)
    with open(path_old_file, 'w') as f:
        for item in next_level_handles:
            f.write("%s\n" % item)
    return


def repopulate_handles(conf):
    handles = get_next_level_handles(conf['db_credentials']['dbname'], conf['db_credentials']['dbuser'],
                                     conf['db_credentials']['dbpass'], conf['db_credentials']['dbhost'],
                                     conf['db_credentials']['dbport'])
    write_next_handles(handles, conf['handles'])
    return


def get_uncrawled_handles(trending_topics):
    current_queries = [trend['name'] for trend in trending_topics[0]['trends']]
    try:
        crawled_queries = []
        with open(PICKLE_FILE_CRAWLED_DATA, 'r') as f:
            crawled_queries = [x for x in f.read()]
        queries_to_crawl = list(set(current_queries).difference(set(crawled_queries)))
        if len(queries_to_crawl) <= 0:
            sys.exit("No new queries to crawl, exiting")
        return queries_to_crawl
    except (OSError, IOError, FileNotFoundError) as e:
        open(PICKLE_FILE_CRAWLED_DATA, 'a').close()
        return current_queries


def get_trending_handles(auth_dict):
    api = init_twitterAPI(auth_dict[0])
    trending_topics = api.trends_place(INDIA_ID_YAHOO)
    trending_topics = get_uncrawled_handles(trending_topics)
    trending_topics_q = Queue()
    [trending_topics_q.put(trend) for trend in trending_topics]
    return trending_topics_q


def get_conf_user():
    configuration = {}
    parser = argparse.ArgumentParser(description="Multi-Threaded crawler for crawling Twitter")
    parser.add_argument("--authcsv", default=None,
                        help="The path to the csv file containing authorization tokens for twitter")
    parser.add_argument("--dbname", default=None, help="Postgres database in which to enter the crawled data")
    parser.add_argument("--dbuser", default=None, help="User name for the Postgres database ")
    parser.add_argument("--threads", default=1, help="Number of threads to use for crawling")
    parser.add_argument("--handles", default='./handles.txt',
                        help="Path to file containing the handles(each on newline) to crawl")
    parser.add_argument("--folder", default='./tweets/',
                        help="Path to folder where tweets CSV file would be dumped")
    parser.add_argument("-r", default=None,
                        help="Populate the handles file, pass anything as value", action='store_true')
    parser.add_argument("-trending", default=False, help="Crawl tweets for currently trending hashtags",
                        action="store_true")
    args = parser.parse_args()
    if len(sys.argv) <= 3:
        conf = get_conf_file()
        if args.r:
            repopulate_handles(conf)
            sys.exit("Reset the handles file successfully")
        if args.trending:
            conf['trending'] = True
            logging.info("Crawling trending tweets")
        else:
            conf['trending'] = False
        return conf
    configuration["threads"] = args.threads
    configuration["target_folder"] = args.folder
    if not args.handles:
        parser.error("No handles file specified !")
    else:
        configuration['handles'] = args.handles
    if bool(args.dbname) ^ bool(args.dbuser):  # check if only one of db parameter is set. Used XOR
        parser.error("Both --dbname and --dbuser should be set, you've set only one of them")
    if args.dbname:
        configuration['db_credentials'] = {'dbname': args.dbname, 'dbuser': args.dbuser, 'dbpass': getpass.getpass(
            prompt="Please enter password for the user of the postgres database ")}
    else:
        configuration['db_credentials'] = None
    if not args.authcsv:
        auth_dct = get_user_input('twitterauth')
        configuration['authcsv'] = [auth_dct]
    else:
        configuration['authcsv'] = get_credentials(args.authcsv)
    return configuration


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    configuration = get_conf_user()
    init_crawler(no_of_threads=configuration["threads"], auth_list=configuration['authcsv'],
                 db_credentials=configuration['db_credentials'],
                 handles_file=configuration['handles'],
                 target_folder=configuration["target_folder"],
                 trending=configuration['trending'])

# TODO: reading from csv files for auth credentials can also be optimized using pandas
# TODO: check for robust handling of in memory data. Can be a problem in case of large crawls.
# TODO: Provide a sample systemd service file configuration
# TODO: Add search by hashtags
# TODO: add configuration file option
# TODO: Use Docker Compose
# TODO: put config methods in a helper class
# TODO: add elastic search to postgres as backend
# TODO: filter directly from database itself by crawled and to crawl ID

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
        logging.error("Cannot read credentials from file: File Not found")
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
        conn = psycopg2.connect(database=database,
                                user=user, password=password, host='headless.nick', port='5432')
        conn.autocommit = True
        return conn
    except psycopg2.DatabaseError as e:
        logging.error("Problem Connecting to database:  " + str(e))


def insert_into_postgres(posts, conn):
    for item in posts:
        keys = list(item.keys())
        values = [item[x] for x in keys]
        #     print(keys,values)
        try:
            cur = conn.cursor()
            cur.execute('insert into tweet_articles_tweepy(%s) values %s;',
                        (psycopg2.extensions.AsIs(','.join(keys)), tuple(values)))
            cur.close()
        except psycopg2.DatabaseError as e:
            logging.critical("Insert Failed " + str(e))


def init_twitterAPI(dct):
    consumer_key = dct['consumer_key']
    consumer_secret = dct['consumer_secret']
    access_token = dct['access_token']
    access_token_secret = dct['access_token_secret']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.auth.API(auth, wait_on_rate_limit=True)
    return api


def crawl_twitter(curr_id, api, conn, output_folder):
    try:
        posts = []
        logging.info("Crawling handle " + curr_id)
        for post in tweepy.Cursor(api.user_timeline, id=curr_id, summary=False, tweet_mode="extended").items():
            dc = {}
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
        if conn:
            insert_into_postgres(posts, conn)
        else:
            if not os.path.exists(output_folder):
                os.mkdir(output_folder)
            csv_file = os.path.join(output_folder, curr_id + ".csv")
            df = pd.DataFrame(posts)
            df.to_csv(csv_file)
    except tweepy.error.TweepError as e:
        logging.error("Can't crawl ID " + str(curr_id) + " exception: " + str(e))


def process(q, api, conn, output_csv):
    while not q.empty():
        curr_id = q.get().split()[0]
        crawl_twitter(curr_id, api, conn, output_csv)
        q.task_done()


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


def init_crawler(no_of_threads, auth_list, db_credentials, handles_file, target_folder):
    q = get_queue(handles_file);
    conn = pg_get_conn(database=db_credentials["dbname"], user=db_credentials["dbuser"],
                       password=db_credentials["dbpass"]) if db_credentials else None
    for i in range(int(no_of_threads)):
        api = init_twitterAPI(auth_list[i % len(auth_list)])
        worker = Thread(target=process, args=(q, api, conn, target_folder))
        worker.start()


def get_user_input(input_type):
    if type == "twitterauth":
        auth_dct = {'consumer_key': getpass.getpass(prompt="Enter the consumer key obtained from twitter"),
                    'consumer_secret': getpass.getpass(prompt="Enter the consumer secret for twitter API"),
                    'access_token': getpass.getpass(prompt="Enter the access token for twitter API"),
                    'access_token_secret': getpass.getpass(prompt="Enter the access token secret for twitter API")}
        return auth_dct


if __name__ == "__main__":
    configuration = {}
    if os.path.exists("./tweet.ini"):
        config = configparser.ConfigParser()
        config.read('./tweet.ini')
        sections = config.sections()
        if 'Database' in sections:
            section = "Database"
            configuration['db_credentials'] = {'dbname': config.get(section, 'dbname'),
                                               'dbuser': config.get(section, 'dbuser'),
                                               'dbpass': config.get(section, 'dbpass'),
                                               'dbhost': config.get(section, 'dbhost'),
                                               'dbport': config.get(section, 'dbport')}
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

    else:
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
        args = parser.parse_args()
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

    init_crawler(no_of_threads=configuration["threads"], auth_list=configuration['authcsv'],
                 db_credentials=configuration['db_credentials'],
                 handles_file=configuration['handles'],
                 target_folder=configuration["target_folder"])

# TODO: reading from csv files for auth credentials can also be optimized using pandas
# TODO: check for robust handling of in memory data. Can be a problem in case of large crawls.
# TODO: Provide a sample systemd service file configuration
# TODO: Add search by hashtags
# TODO: add configuration file option
# TODO: Use Docker Compose
# TODO: put config methods in a helper class

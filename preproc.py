import logging
import os
import pickle
import sqlite3
import urllib.request
from collections import OrderedDict

import networkx as nx
import psycopg2
import requests
import tweepy
from bs4 import BeautifulSoup


def get_pickle(file):
    """Get Pickle file

    Args:
        file (str): Location of file

    Returns:
        object: the object read
    """
    with open(file, 'rb') as f:
        ls = pickle.load(f)
    return ls


def get_political_handles(list_file=['files/inc_handles.txt', 'files/bjp_handles.txt'], get_online=True):
    """Get the political handles from files as well as socialbakers site
    Returns:
        list: list of twitter handles
    Args:
        list_file (list, optional): List of files from which to get handles
        get_online (bool, optional): Whether or not to get info online
    """
    ls_fin = []
    for i in range(1, 23):
        url = "https://www.socialbakers.com/statistics/twitter/profiles/india/society/politics/page-" + \
              str(i) + "/?showMoreList-from=1&do=platformList-renderAjax&json"
        html = urllib.request.urlopen(url)
        soup = BeautifulSoup(html, 'html.parser')
        intm = soup.find_all('h2')
        for y in intm:
            for x in y.find_all('span'):
                ls_fin.append(x.text.split(
                    '(')[-1].replace(')', '').replace('@', ''))
    for file in list_file:
        with open(file) as f:
            for i in f:
                if i:
                    ls_fin.append(i.strip())
    logging.info(str(len(ls_fin)) + " IDs crawled")
    return (ls_fin)



def crawl_twitter(list_ids, api, wait_on_rate_limit=False):
    """Crawl twitter using official twitter API. Inserts into postgres database

    Args:
        list_ids (list): List of twitter handles to be crawled
        api (tweepyAPI object): Tweepy api object initialized appropriately
        wait_on_rate_limit (bool, optional): wait on exhaustion of quota and continue once replenished

    Returns:
        list: list of dictionaries representing tweets

    Deleted Parameters:
        db (bool, optional): Insert into database
    """

    ldc = []
    count = 0
    try:
        for curr_id in list_ids:
            for post in tweepy.Cursor(api.user_timeline, id=curr_id, summary=False, tweet_mode="extended",
                                      wait_on_rate_limit=wait_on_rate_limit).items():
                dc = OrderedDict()
                curr_post = post._json
                dc['tweet_from'] = curr_id
                dc['created_at'] = curr_post['created_at']
                dc['hashtags'] = [x['text']
                                  for x in curr_post['entities']['hashtags']]
                dc['urls'] = [x['expanded_url']
                              for x in curr_post['entities']['urls']]
                dc['user_mentions_id'] = [x['id']
                                          for x in curr_post['entities']['user_mentions']]
                if 'media' in curr_post['entities']:
                    dc['media'] = [x['media_url_https']
                                   for x in curr_post['entities']['media']]
                dc['user_mentions_name'] = [x['screen_name']
                                            for x in curr_post['entities']['user_mentions']]
                dc['origin_device'] = BeautifulSoup(
                    curr_post['source'], 'html.parser').a.string
                dc['favorite_count'] = curr_post['favorite_count']
                dc['text'] = curr_post['full_text']
                dc['id'] = curr_post['id']
                dc['in_reply_to_screen_name'] = curr_post[
                    'in_reply_to_screen_name']
                dc['in_reply_to_user_id'] = curr_post['in_reply_to_user_id']
                dc['in_reply_to_status_id'] = curr_post[
                    'in_reply_to_status_id']
                dc['retweet_count'] = curr_post['retweet_count']
                #         adding retweet information because it is important.
                if ('retweeted_status' in curr_post):
                    dc['retweeted_status_text'] = curr_post[
                        'retweeted_status']['full_text']
                    dc['retweeted_status_url'] = [x['expanded_url']
                                                  for x in curr_post['retweeted_status']['entities']['urls']]
                    dc['retweeted_status_id'] = curr_post[
                        'retweeted_status']['id']
                    dc['retweeted_status_user_name'] = curr_post[
                        'retweeted_status']['user']['name']
                    dc['retweeted_status_user_handle'] = curr_post[
                        'retweeted_status']['user']['screen_name']
                ldc.append(dc)
                count += 1
    except Exception as twe:
        print(str(twe))
    print("Total count : " + str(count))
    return (ldc)


def initialize_sqlite(file='twitter.db'):
    """Initialize SQLITE connection

    Args:
        file (str, optional): Location of sqlite file

    Returns:
        sqlite connection: Sqlite connection object
    """
    conn = sqlite3.connect(file)
    return conn


def preproc_db(ldc):
    """Preprocess the crawled list of dicts from twitter API to insert into DB

    Args:
        ldc (list): List of dicts containing crawled data from twitter

    Returns:
        list: list fo dicts ready to be inserted in database
    """
    for dc in ldc:
        if (isinstance(dc['hashtags'], str)):
            print("Already PreProcessed")
            return ldc
        if 'retweeted_status_url' in dc:
            dc['retweeted_status_url'] = ",".join(dc['retweeted_status_url'])
        if 'hashtags' in dc and dc['hashtags']:
            #         print(dc['hashtags'])
            dc['hashtags'] = ",".join(dc['hashtags'])
        else:
            dc['hashtags'] = 'NULL'

        if 'urls' in dc:
            dc['urls'] = ",".join(dc['urls'])
        else:
            dc['urls'] = 'NULL'

        if 'media' in dc:
            dc['media'] = ",".join(dc['media'])
        else:
            dc['media'] = 'NULL'
        if 'user_mentions_id' in dc:
            dc['user_mentions_id'] = ','.join(
                str(x) for x in (dc['user_mentions_id']))
        else:
            dc['user_mentions_id'] = 'NULL'
        if 'user_mentions_name' in dc:
            dc['user_mentions_name'] = ",".join(dc['user_mentions_name'])
        else:
            dc['user_mentions_name'] = 'NULL'
    return (ldc)


def insert_into_db_list(ldc):
    count = 0
    for dc in ldc:
        try:
            columns = ', '.join(dc.keys())
            placeholders = ':' + ', :'.join(dc.keys())
            query = 'INSERT INTO TWITTER (%s) VALUES (%s)' % (
                columns, placeholders)
            conn.execute(query, dc)
        except sqlite3.IntegrityError as e:
            count += 1
    print("Duplicates: " + str(count))
    conn.commit()


def create_table_db(conn):
    conn.execute('''CREATE TABLE TWITTER
         (
         id INT PRIMARY KEY NOT NULL,
         tweet_from TEXT    NOT NULL,
         created_at            BLOB     NOT NULL,
         hashtags        TEXT,
         urls         TEXT,
         user_mentions_id TEXT,
         media TEXT,
         user_mentions_name TEXT,
         origin_device TEXT,
         favorite_count INT,
         text TEXT,

         in_reply_to_screen_name TEXT,
         in_reply_to_user_id INT,
         in_reply_to_status_id INT,
         retweet_count INT,
         retweeted_status_text TEXT,
         retweeted_status_url BLOB,
         retweeted_status_id INT,
         retweeted_status_user_name TEXT,
         retweeted_status_user_handle TEXT

         );''')


def construct_graph(edge_list, G=nx.DiGraph()):
    for t in edge_list:
        if (len(t) == 2):
            if (G.has_edge(t[0], t[1])):
                G[t[0]][t[1]]['weight'] += 1
            else:
                G.add_edge(t[0], t[1], weight=1)
    return (G)


def get_list_ids_to_crawl_next(combined_id, list_ids=[], idx=1):
    new_list_to_crawl = []
    for x in combined_id:
        if (x[idx]):
            for y in x[idx].split(','):
                new_list_to_crawl.append(y)
    if (list_ids):
        new_list_to_crawl = list(set(new_list_to_crawl) - (set(list_ids)))
    return (new_list_to_crawl)


def crawl(combined_id):
    for index, x in enumerate(combined_id):
        if (x):
            combined_id[index] = "from:" + x
    if not combined_id[0]:
        combined_id.pop(0)
    succ_cnt = 0
    for c in combined_id:
        print("Current id -> " + c + "\n Success Count -> " + str(succ_cnt))
        rs = os.system(
            "cd ../TweetScraper; scrapy crawl TweetScraper -a query=\"" + c + "\"")
        if (rs == 0):
            id_crawled.append(c)
            succ_cnt += 1
    return (succ_cnt)


def construct_edge_list(cong):
    """
    Constructs edge lists with two mappings user->user mapping and user-websource mapping

    Paramaters:
        cong - list of tuples with 3 fields(user_from,user_mention,link)
    returns:
        user-user edge list, user-source edge
    """
    usr_to_src = []
    list_to_exclude = [
        'twitter',
        'youtu',
        'fllwrs',
        'unfollowspy',
        'livetv',
        'pscp',
        'live',
        'ln.is',
        'tinyurl',
        'facebook',
        'bit.ly',
        'goo.gl',
        'instagram',
        'google'
    ]
    for x in cong:
        if x[2]:
            for url in x[2].split(','):
                if not any(y in url for y in list_to_exclude) and x[0] not in url.replace('.', '_'):
                    if url.endswith('.com') and url.startswith("http://www"):
                        usr_to_src.append((x[0], url.split('.')[1].lower()))
                    elif url.endswith('.com') and url.startswith("http://m"):
                        usr_to_src.append((x[0], url.split('.')[1].lower()))
                    elif url.endswith('.in') and url.startswith("http://www"):
                        usr_to_src.append((x[0], url.split('.')[1].lower()))
                    elif url.startswith("http://") or url.startswith("https://"):
                        l_url = url.split('/')
                        if len(l_url) >= 3 and '.' in l_url[2]:
                            if l_url[2].startswith('www') or l_url[2].startswith('m'):
                                usr_to_src.append(
                                    (x[0], l_url[2].split('.')[1].lower()))
                            else:
                                usr_to_src.append((x[0], l_url[2].lower()))

    ll = []
    for i in cong:
        if i[1]:
            for x in i[1].split(','):
                if (x != '@'):
                    x = x.replace('@', '')
                    ll.append((i[0], x))
    return (ll, usr_to_src)


def unshorten_url(url):
    return requests.head(url, allow_redirects=True).url


def pg_get_conn(database="fakenews", user="fakenews", password="fnd"):
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
        print(str(e))




def get_clean_urls(text_list, list_to_exclude=['twitter']):
    """
    Get clean URLS from tweet texts from TweetScraper, takes into account urls broken by intermittent spaces
    Parameters:
    text_list - List of tweets containing tuples of (id, text)
    list_to_exclude - List of sites to exclude
    Returns:
    ans_ls - List of tuples of type (id,urls)
    """
    ans_ls = []
    for x in text_list:
        rex = re.findall(
            '(?:http:|https:)\/\/.*\/.*?(?:\.cms|\.[a-zA-Z]*|\/[a-zA-Z0-9-\ ]+[a-zA-z0-9])', x[1])
        for rx in rex:
            if rx and not any(z in rx for z in
                              list_to_exclude) and not rx == 'http://' and not rx == 'https://' and not rx.endswith(
                '.') and 't.c' not in rx:
                if '\xa0' in x[1]:
                    for y in x[1].split('\xa0'):
                        #             print(x[0],y)
                        ans_ls.append((x[0], y.replace(' ', '')))
                elif '@' in x[1]:
                    ans_ls.append((x[0], y.split('@')[0].replace(' ', '')))

                else:
                    ans_ls.append((x[0], x[1].replace(' ', '')))
    return (ans_ls)

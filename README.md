# TweetCrawlerMultiThreaded
This is small tool which I developed during my days at IITB to enable crawling twitter using a list of twitter handles. This tool is designed to crawl large amounts of tweets using multiple credentials. 
This connects to a postgres database to dump all the tweets alternatively you can dump the crawled data into a csv files which are organized by users. 

## Configuration
You can enter the configuration using either command line or using a .ini file. Following is a sample configuration file 
```bash
[Database]
dbname="dbname"
dbuser="username"
dbpass="password"
dbhost="localhost" #If you're db is hosted on a different server put it's domain name here
dbport="5432"
[Twitter]
authCSV="./twitteraccesscodes.csv" #The path to CSV file containing twitter access tokens in the format specified
handlesFile="./handle.txt"
[System]
Threads="5"
CSVFolder="./tweets/"
```
You can save this configuration in the same folder as tweet.ini. You can also enter all this information using command line. Running the program along with '-h' parameter will list all the options and arguments.

The database schema can be replicated using the following SQL command

```sql
CREATE TABLE public.tweet_articles_tweepy (
    id bigint NOT NULL,
    tweet_from text,
    created_at character varying,
    hashtags text,
    urls text,
    user_mentions_id text,
    media text,
    user_mentions_name text,
    origin_device text,
    favorite_count bigint,
    text text,
    in_reply_to_screen_name text,
    in_reply_to_user_id bigint,
    in_reply_to_status_id bigint,
    retweet_count bigint,
    retweeted_status_text text,
    retweeted_status_url character varying,
    retweeted_status_id bigint,
    retweeted_status_user_name text,
    retweeted_status_user_handle text,
    sentiment numeric
);
ALTER TABLE ONLY public.tweet_articles_tweepy
    ADD CONSTRAINT tweet_articles_tweepy_pkey PRIMARY KEY (id);
```

### Update:
Added a crawl trending option to crawl all the trending tweets. Run the program with -trending argument 
```bash
python3 TweetCrawler.py -trending
```

### Bug:
After adding the code for Trending handles I found out that the crawler had a memory leak which would eat up the whole memory if left unattended.
I tried to trouble shoot it but there is no eay way to profile memory in Python. Finally looking at the code it became clear that the crawling part was clean.
I then looked into the postgres part where the first bug was identified right away. The loop which was inserting values in the table was creating a new cursor object every iteration which would've caused the mmeory to balloon. Also it didn't make a lot of sense. So I fixed that.

Next problem was since there was a single connection for all the threads, (which is valid btw according to docs) this causes all the inserts to be serialised and since our code was inserting every tweet crawled this caused a lot of slowdown. Instead we shifted to one connection per thread.
Even in that case I suspect since we didn't explicitly call close() on connection object, the reference to that connection object was still in memory. So instead for every handle we create a new connection and close it after crawling, this should fix the memory bug but testing is still pending.

Even after these optimization the memory utilization increased with time. The original approach to threading was naive and was intended as proof of concept, I changed 
that to a Thread pool model. One of the reasons I suspect the original implementation ballooned in memory was because of threads running in loop. 
As the threads never ended before the crawling handles were exhausted I think GC wasn't kicking in. So instead, we move to a Thread pool model where the threads run and crawl each handle and after they are done they are released to the pool.
Hopefully this should resolve the issues with memory.  

### Migrating to MultiProcess model
Given that the Multithreaded model for our crawler is always going to be handicapped by the GIL of python. I thought it'd be better
if I move the crawler to a multithreaded model. Since we have already moved to the thread pool model the change is simple as changin one line in the code. 
The only problem is this breaks the crawler for MacOS due to a known [limitation](https://stackoverflow.com/questions/55286016/python-is-crashing-due-to-libdispatch-crashing-child-thread) of Mac OS. I'll probably create a separate branch for the Mac OS
#### Finally solved memory leak
The memory leak was largely due to the executor.map holding on to futures objects in memory. The executor map function holds in memory all the related objects till all inputs are processed. 
This becomes problematic in our case since the amount of crawled data is basically unlimited. The program holds all this data in memory which causes the memory consumption of program to ballon.
The solution in this case is using the __chunksize__ parameter, which maps the data in chunks, this causes the executor object to release memory before continuing with the next chunk.
  
 
 
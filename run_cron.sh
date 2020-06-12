#!/bin/bash
pkill -f trending;
cd /home/abhishek/Documents/TweetCrawlMultiThreaded;
source /home/abhishek/Documents/TweetCrawlMultiThreaded/venv/bin/activate;
python3 /home/abhishek/Documents/TweetCrawlMultiThreaded/TweetCrawler.py -trending >> /var/log/twitterCrawler.log 2>&1;


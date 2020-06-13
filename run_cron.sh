#!/bin/bash
pkill -f trending;
wc -l /home/abhishek/Documents/TweetCrawlMultiThreaded/crawled.txt > handle_counter.txt;
cd /home/abhishek/Documents/TweetCrawlMultiThreaded;
source /home/abhishek/Documents/TweetCrawlMultiThreaded/venv/bin/activate;
python3 /home/abhishek/Documents/TweetCrawlMultiThreaded/TweetCrawler.py -trending &;



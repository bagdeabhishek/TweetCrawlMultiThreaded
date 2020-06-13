#!/usr/bin/python3
import time
initial_count=0
with open('/home/abhishek/Documents/TweetCrawlMultiThreaded/handle_counter.txt','r',encoding="utf8") as f:
    for t in f:
        initial_count = int(t.split()[0]);
handles_crawled = 0
curr_count = 0
with open('/home/abhishek/Documents/TweetCrawlMultiThreaded/crawled.txt','r',encoding="utf8") as f:
    for x in f:
        curr_count+=1
if handles_crawled != (curr_count - initial_count):
    handles_crawled = (curr_count - initial_count)
    print(f"Crawled {handles_crawled} handles")

   



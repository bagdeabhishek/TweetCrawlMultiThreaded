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


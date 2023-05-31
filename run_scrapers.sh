#/bin/bash
# Runs all scrapers

now=`date +%m-%d-%Y-%H-%M`

# Eg: scrapy crawl tentimes -o tentimes-2023-05-28-raw.json -a urlindex=5 -a max_urls_per_category=2
scrapy crawl $1 -o $1-$now-raw.json -a max_urls_per_category=10 -a urlindex=1

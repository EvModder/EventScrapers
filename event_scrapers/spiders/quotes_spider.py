from pathlib import Path

import logging
import scrapy


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def __init(self, *args, **kwargs):
        logging.getLogger('scrapy').propagate = True
        super().init(*args, **kwargs)

    def start_requests(self):
        self.log('Starting requests')
        urls = [
            "https://quotes.toscrape.com/page/1/",
            "https://quotes.toscrape.com/page/2/",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = f"quotes-{page}.html"
        Path(filename).write_bytes(response.body)
        self.log(f"Saved file {filename}")

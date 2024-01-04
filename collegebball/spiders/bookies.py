import scrapy
import json
from collegebball.items import BookieItem
class BookiesSpider(scrapy.Spider):
    name = 'bookies'
    start_urls=[]
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_bookie)

    def parse_bookie(self, response):
        data = json.loads(response.body)
        bookie = BookieItem()
        
        bookie["bookie_ref"] = data["$ref"]
        bookie["id"] = data["id"]
        bookie["name"] = data["name"]
        bookie["priority"] = data["priority"]

        if bookie["name"] == "Not Available":
            pass
        else:
            yield bookie
import scrapy


class SeasonsSpider(scrapy.Spider):
    name = "seasons"
    def __init__(self, *args, **kwargs):
        super(SeasonsSpider, self).__init__(*args, **kwargs)

        endpoints = kwargs.get("start_urls").split(",")
        
        #populate start_urls with the endpoints
        self.start_urls = []
        for endpoint in endpoints:
            self.start_urls.append(endpoint)


        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_event)


    def parse(self, response):
        pass

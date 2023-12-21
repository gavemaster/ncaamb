import scrapy


class RostersSpider(scrapy.Spider):
    name = "rosters"
    start_urls = []  # This will be populated from the script

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url["event_ref"],
                callback=self.parse_roster,
                meta={
                    "season": url["season"],
                    "season_type": url["season_type"],
                    "week": url["week"],
                },
            )

    def parse(self, response):
        pass

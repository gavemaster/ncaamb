import scrapy

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import collegebball.utils as utils


from collegebball.items import ConferenceItem

class ConferencesSpider(scrapy.Spider):
    name = "conferences"
    start_urls = [] 
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_conference
            )
    def parse_conference(self, response):

        data = json.loads(response.body)

        conference_item = ConferenceItem()

        conference_item["conference_id"] = data["id"]
        conference_item["name"] = data["name"]

        if "short_name" in data:
            conference_item["short_name"] = data["short_name"]
        else:
            conference_item["short_name"] = None

        if "standings" in data:
            conference_item["conference_standings_ref"] = data["standings"]["$ref"]
        else:
            conference_item["conference_standings_ref"] = None

        if "teams" in data:
            conference_item["conference_teams_ref"] = data["teams"]["$ref"]
        else:
            conference_item["conference_teams_ref"] = None

        conference_item["season"] = utils.extract_year_from_url(response.url)

        yield conference_item



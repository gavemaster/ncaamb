import argparse
import os
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.events import EventsSpider


event_urls = []
parser = argparse.ArgumentParser(description="Run the events spider")
parser.add_argument(
    "-start_season", type=int, help="Starting Season to run the spider for"
)
parser.add_argument("-end_season", type=int, help="Ending Season to run the spider for")

args = parser.parse_args()

# start_season = args.start_season
# end_season = args.end_season


event_urls = utils.get_event_urls(2023, 2023)


print(event_urls)


# process = CrawlerProcess(get_project_settings())

import concurrent.futures

import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.bookies import BookiesSpider
import datetime as dt


start = dt.datetime.now()


bookie_links = utils.get_links_from_multiple_pages("http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/providers")


print("bookie links:", bookie_links)

print("starting spider")


#utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(BookiesSpider, start_urls=bookie_links)
process.start()

#utils.stop_spinner()

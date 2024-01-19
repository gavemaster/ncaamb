import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.event_odds_lines import EventOddsLinesSpider 
import datetime as dt

start = dt.datetime.now()

event_odds_lines_links = db.get_event_odds_lines_links()

print("starting spider")


utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(EventOddsLinesSpider, start_urls=event_odds_lines_links)
process.start()

utils.stop_spinner()

end = dt.datetime.now()

print("Time elapsed:", end - start)
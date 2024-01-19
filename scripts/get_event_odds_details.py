import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.event_odds_details import EventOddsDetailsSpider 
import datetime as dt

start = dt.datetime.now()

event_odds_details_links = db.get_event_odds_details_links()

print("starting spider")


utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(EventOddsDetailsSpider, start_urls=event_odds_details_links)
process.start()

utils.stop_spinner()

end = dt.datetime.now()

print("Time elapsed:", end - start)
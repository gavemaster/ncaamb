import concurrent.futures
import argparse
import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.event_odds import EventOddsSpider 
import datetime as dt


#/bin/python /home/hoff/dev/web-scraping/ncaamb/scripts/get_event_odds.py 2023-11-15 2024-01-04

start = dt.datetime.now()

# take in a start date and end date as arguments
parser = argparse.ArgumentParser()
parser.add_argument("start_date", type=str)
parser.add_argument("end_date", type=str)
args = parser.parse_args()


if args.start_date > args.end_date:
    raise ValueError("Start date must be less than or equal to end date")
elif args.start_date is None or args.end_date is None:
    raise ValueError("Start date and end date must be provided")

valid_start = utils.check_date_format(args.start_date)
valid_end = utils.check_date_format(args.end_date)

if not valid_start or not valid_end:
    raise ValueError("Start date and end date must be in YYYY-MM-DD format")





event_odds = db.get_event_odds(args.start_date, args.end_date)



print("starting spider")


utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(EventOddsSpider, start_urls=event_odds)
process.start()

utils.stop_spinner()

end = dt.datetime.now()

print("Time elapsed:", end - start)
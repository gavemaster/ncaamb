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
parser.add_argument("start_year", type=int)
parser.add_argument("end_year", type=int)
args = parser.parse_args()


if args.start_year > args.end_year:
    raise ValueError("Start year must be less than or equal to end year")
elif args.start_year < 2002:
    raise ValueError("Start year must be greater than or equal to 2002")
elif args.end_year > 2024:
    raise ValueError("End year must be less than or equal to 2024")
elif args.start_year is None or args.end_year is None:
    raise ValueError("Start year and end year must be provided")


event_odds = db.get_event_odds(args.start_year, args.end_year)



print("starting spider")


utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(EventOddsSpider, start_urls=event_odds)
process.start()

utils.stop_spinner()

end = dt.datetime.now()

print("Time elapsed:", end - start)
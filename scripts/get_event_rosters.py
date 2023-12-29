import concurrent.futures
import argparse
import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.rosters import RostersSpider 
import datetime as dt






start = dt.datetime.now()
# take in a start year and end year as arguments
parser = argparse.ArgumentParser()
parser.add_argument("start_year", type=int)
parser.add_argument("end_year", type=int)
args = parser.parse_args()


print("Getting event rosters for events between {} and {}".format(args.start_year, args.end_year))


if args.start_year > args.end_year:
    raise ValueError("Start year must be less than or equal to end year")
elif args.start_year < 2002:
    raise ValueError("Start year must be greater than or equal to 2002")
elif args.end_year > 2024:
    raise ValueError("End year must be less than or equal to 2024")
elif args.start_year is None or args.end_year is None:
    raise ValueError("Start year and end year must be provided")

event_rosters = db.get_event_rosters(args.start_year, args.end_year)


print("starting spider")


#utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(RostersSpider, start_urls=event_rosters)
process.start()

#utils.stop_spinner()

# runtime in seconds
end = dt.datetime.now()
runtime = end - start
print(f"runtime (seconds):  {runtime.total_seconds()}")
runtime_minutes = runtime.total_seconds() / 60
rounded_runtime = round(runtime_minutes, 1)
print(f"runtime (minutes): {rounded_runtime}")

print("done")

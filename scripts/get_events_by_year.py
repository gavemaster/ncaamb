import concurrent.futures
import json
import requests
import collegebball.database as db
import collegebball.utils as utils
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.events import EventsSpider



# take in a start year and end year as arguments
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

event_landing_pages = db.get_event_landing_pages_from_db(args.start_year, args.end_year)


def fetch_event_data(event):
    event_data = utils.fetch_data(event["week_events_ref"])
    if not event_data:
        return []
    page_count = event_data["pageCount"]
    event_links = []
    for i in range(1, page_count + 1):
        event_url = event["week_events_ref"] + "&page=" + str(i)
        links = utils.get_event_urls_from_page(
            event["season"], event["season_type"], event["week"], event_url
        )
        event_links.extend(links)
    return event_links


events = []

# Use ThreadPoolExecutor to fetch data concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_event = {
        executor.submit(fetch_event_data, event): event for event in event_landing_pages
    }
    for future in concurrent.futures.as_completed(future_to_event):
        events.extend(future.result())

print("Number of events:", len(events))


print("starting spider")

process = CrawlerProcess(get_project_settings())
process.crawl(EventsSpider, start_urls=events)
process.start()

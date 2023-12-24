import concurrent.futures
import argparse
import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.athletes import AthletesSpider
import datetime as dt

start = dt.datetime.now()
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

athlete_landing_pages = db.get_athlete_landing_pages(args.start_year, args.end_year)


def fetch_athletes_data(athlete):
    athlete_data = utils.fetch_data(athlete["athletes_ref"])
    if not athlete_data:
        return []
    page_count = athlete_data["pageCount"]
    event_links = []
    for i in range(1, page_count + 1):
        _url = athlete["athletes_ref"] + "&page=" + str(i)
        links = utils.get_athlete_urls_from_page(athlete["season"], _url)
        event_links.extend(links)
    return event_links


athletes = []

# Use ThreadPoolExecutor to fetch data concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_athletes = {
        executor.submit(fetch_athletes_data, athletes): athletes
        for athletes in athlete_landing_pages
    }
    for future in concurrent.futures.as_completed(future_to_athletes):
        athletes.extend(future.result())


print("Number of athletes:", len(athletes))


print("starting spider")


utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(AthletesSpider, start_urls=athletes)
process.start()

utils.stop_spinner()

# runtime in seconds
end = dt.datetime.now()
runtime = end - start
print(f"runtime (seconds):  {runtime.total_seconds()}")
runtime_minutes = runtime.total_seconds() / 60
rounded_runtime = round(runtime_minutes, 1)
print(f"runtime (minutes): {rounded_runtime}")

print("done")

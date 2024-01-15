import concurrent.futures
import json
import requests
import collegebball.database as db
import collegebball.utils as utils
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.conferences import ConferencesSpider
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

urls= []

for year in range(args.start_year, args.end_year + 1):
    url_str = f"http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/{year}/types/3/groups/50/children?lang=en&region=us"
    urls.append(url_str)


def fetch_conf_data(url):
    conf_data = utils.fetch_data(url)
    if not conf_data:
        return []
    page_count = conf_data["pageCount"]
    conf_links = []
    for i in range(1, page_count + 1):
        conf_url = url + "&page=" + str(i)
        links = utils.get_conference_urls_from_page(conf_url)
        conf_links.extend(links)
    return conf_links


conferences = []

# Use ThreadPoolExecutor to fetch data concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_event = {
        executor.submit(fetch_conf_data, conf): conf for conf in urls
    }
    for future in concurrent.futures.as_completed(future_to_event):
        conferences.extend(future.result())




print("starting spider")


#utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(ConferencesSpider, start_urls=conferences)
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


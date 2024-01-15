import concurrent.futures
import json
import requests
import collegebball.database as db
import collegebball.utils as utils
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.team_spider import TeamSpiderSpider
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
    url_str = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/{year}/teams?lang=en&region=us"
    urls.append(url_str)


def fetch_team_data(url):
    team_data = utils.fetch_data(url)
    if not team_data:
        return []
    page_count = team_data["pageCount"]
    team_links = []
    for i in range(1, page_count + 1):
        team_url = url + "&page=" + str(i)
        links = utils.get_team_urls_from_page(team_url)
        if links is None:
            continue
        else:
            team_links.extend(links)
    return team_links


teams = []

# Use ThreadPoolExecutor to fetch data concurrenty
with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_event = {
        executor.submit(fetch_team_data, team): team for team in urls
    }
    for future in concurrent.futures.as_completed(future_to_event):
        teams.extend(future.result())




print("starting spider")



process = CrawlerProcess(get_project_settings())
process.crawl(TeamSpiderSpider, start_urls=teams)
process.start()

print("spider finished")
    
end = dt.datetime.now()

print(f"Time elapsed: {end - start}")

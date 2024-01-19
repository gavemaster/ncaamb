import concurrent.futures
import argparse
import collegebball.database as db
import collegebball.utils as utils
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.athletes import AthletesSpider
import datetime as dt

start = dt.datetime.now()



def fetch_athletes_data(athlete):
    athlete_data = utils.fetch_data(athlete)
    if not athlete_data:
        return []
    page_count = athlete_data["pageCount"]
    event_links = []
    for i in range(1, page_count + 1):
        _url = athlete + "&page=" + str(i)
        links = utils.get_athlete_urls_from_page(_url)
        event_links.extend(links)
    return event_links



links = fetch_athletes_data("https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/athletes?limit=1000")


utils.start_spinner()

process = CrawlerProcess(get_project_settings())
process.crawl(AthletesSpider, start_urls=links)
process.start()


utils.stop_spinner()

end = dt.datetime.now()
print("Time to complete: {}".format(end - start))

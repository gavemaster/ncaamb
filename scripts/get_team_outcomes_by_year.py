import collegebball.database as db

from datetime import datetime
from zoneinfo import ZoneInfo
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.team_outcomes import TeamOutcomesSpider
import datetime as dt

parser = argparse.ArgumentParser()
parser.add_argument("start_year", type=int)
parser.add_argument("end_year", type=int)
args = parser.parse_args()


start = dt.datetime.now()

if args.start_year > args.end_year:
    raise ValueError("Start year must be less than or equal to end year")
elif args.start_year < 2002:
    raise ValueError("Start year must be greater than or equal to 2002")
elif args.end_year > 2024:
    raise ValueError("End year must be less than or equal to 2024")


team_outcomes = db.get_team_ats_link_and_record_link_from_db(args.start_year, args.end_year)




print("starting spider")



process = CrawlerProcess(get_project_settings())
process.crawl(TeamOutcomesSpider, start_urls=team_outcomes)
process.start()

print("spider finished")
    
end = dt.datetime.now()

print(f"Time elapsed: {end - start}")

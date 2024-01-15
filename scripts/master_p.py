
import argparse
import subprocess
import datetime as dt
import collegebball.database as db
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from collegebball.spiders.team_spider import TeamSpiderSpider 

start = dt.datetime.now()

# take in a start year and end year as arguments
parser = argparse.ArgumentParser()
parser.add_argument("start_year", type=int)
parser.add_argument("end_year", type=int)
args = parser.parse_args()


print("Starting master script for college basketball data scraper.... scrapping data for years between {} and {}".format(args.start_year, args.end_year))

print("gathering season links")
season_links_start = dt.datetime.now()
subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_season_links.py"])

time_elapsed=dt.datetime.now() - season_links_start

tot_time_elapsed = dt.datetime.now() - start

print("season links gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering season details")


season_details_start = dt.datetime.now()
subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_season_type_details.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - season_details_start
tot_time_elapsed = dt.datetime.now() - start

print("season details gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering week details")

week_details_start = dt.datetime.now()
subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_weeks_details.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - week_details_start

tot_time_elapsed = dt.datetime.now() - start

print("week details gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering teams data")

teams_start = dt.datetime.now()

process = CrawlerProcess(get_project_settings())
process.crawl(TeamSpiderSpider, start_year=str(args.start_year), end_year=str(args.end_year))
process.start()

time_elapsed=dt.datetime.now() - teams_start


tot_time_elapsed = dt.datetime.now() - start

print("teams data gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering athlete data")

athletes_start = dt.datetime.now()

subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_athletes_by_year.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - athletes_start

tot_time_elapsed = dt.datetime.now() - start

print("athlete data gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering event data")

events_start = dt.datetime.now()

subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_events_by_week.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - events_start

tot_time_elapsed = dt.datetime.now() - start

print("event data gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering event rosters")

event_rosters_start = dt.datetime.now()

subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_event_rosters.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - event_rosters_start

tot_time_elapsed = dt.datetime.now() - start

print("event rosters gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering player stats")

player_stats_start = dt.datetime.now()

subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_player_stats_by_year.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - player_stats_start

tot_time_elapsed = dt.datetime.now() - start

print("player stats gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering bookie data")

bookie_data_start = dt.datetime.now()

subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_bookies.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - bookie_data_start

tot_time_elapsed = dt.datetime.now() - start

print("bookie data gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("gathering event odds")

min_date, max_date = db.get_event_date_range()


event_odds_start = dt.datetime.now()

subprocess.run(["python", "/home/hoff/dev/web-scraping/ncaamb/scripts/get_event_odds.py", str(args.start_year), str(args.end_year)])

time_elapsed=dt.datetime.now() - event_odds_start

tot_time_elapsed = dt.datetime.now() - start

print("event odds gathered in {} seconds... Total runtime: {} seconds".format(time_elapsed.total_seconds(), tot_time_elapsed.total_seconds()))

print("Master Script Complete for college basketball data scraper.... scrapping data for years between {} and {}".format(args.start_year, args.end_year))

tot_time_elapsed = dt.datetime.now() - start
print("Total runtime: {} seconds".format(tot_time_elapsed.total_seconds()))

import argparse
import subprocess
import datetime as dt
import time
from collections import deque

start = dt.datetime.now()

# take in a start year and end year as arguments
parser = argparse.ArgumentParser()
parser.add_argument("start_year", type=int)
parser.add_argument("end_year", type=int)
args = parser.parse_args()


file_names = {"/home/hoff/dev/web-scraping/ncaamb/scripts/get_season_links.py":[], 
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_season_type_details.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_weeks_details.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_conferences_by_year.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_teams_by_year.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_team_outcomes_by_year.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_athletes.py":[],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_events_by_year.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_event_rosters.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_player_stats_by_year.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_bookies.py":[],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_event_odds.py":[str(args.start_year), str(args.end_year)],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_event_odds_details.py":[],
               "/home/hoff/dev/web-scraping/ncaamb/scripts/get_event_odds_lines.py":[],

            }

print("Starting master script for college basketball data scraper.... scrapping data for years between {} and {}".format(args.start_year, args.end_year))


# Convert dictionary to a FIFO queue
queue = deque(file_names.items())

# Process the queue
while queue:
    file_name, args = queue.popleft()
    file_name_name = file_name.split("/")[-1]
    
    print(f"file_name: {file_name}, args: {args}, file_name_name: {file_name_name}")


    #if args is none then run script without args
    if len(args) == 0:
        script_start = dt.datetime.now()
        print("running script: {}".format(file_name_name))
        subprocess.run(["python", file_name])
        script_end = dt.datetime.now()
        
    else:
        script_start = dt.datetime.now()
        print("running script: {}".format(file_name_name))
        subprocess.run(["python", file_name] + args)
        script_end = dt.datetime.now()
        
    
    print(f"Time elapsed for script ( {file_name_name} ): {script_end - script_start}")
    print("Time elapsed since start: {}".format(script_end - start))
    print("Now time for a rest....")
    time.sleep(25)



print("Queue is empty, all scripts have been run....")
end = dt.datetime.now()

print("Time elapsed: ", end - start)



    
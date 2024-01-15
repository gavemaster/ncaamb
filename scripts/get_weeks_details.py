import concurrent.futures
import collegebball.database as db
import json
import requests
import collegebball.utils as utils
import argparse

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


def process_url(url):
    data = utils.fetch_data(url)
    if not data:
        return None

    # Extract the season and type from the URL
    season, type_ = utils.extract_season_and_type(url)
    weeks = data["items"]

    week_details = []

    for week in weeks:
        week_dict = {}
        week_data = utils.fetch_data(week["$ref"])
        if not week_data:
            continue
        
        week_events_ref = week["$ref"]
        # Split the URL at the question mark
        parts = week_events_ref.split('?')

        # Add '/events' to the first part
        modified_url = parts[0] + "/events"

        # Rejoin with the second part, if it exists
        if len(parts) > 1:
            modified_url += '?' + parts[1]

        week_dict["week_events_ref"] = modified_url
        week_dict["season"] = season
        week_dict["type"] = type_
        week_dict["week"] = week_data["number"]
        week_dict["start_date"] = utils.convert_zulu_date_to_est(week_data["startDate"])
        week_dict["end_date"] = utils.convert_zulu_date_to_est(week_data["endDate"])
        week_dict["name"] = week_data["text"]
        if "rankings" in week_data:
            week_dict["rankings_ref"] = week_data["rankings"]["$ref"]
        else:
            week_dict["rankings_ref"] = None
        
        week_details.append(week_dict)

    return week_details


# Main processing with ThreadPoolExecutor
urls = db.get_week_links_from_db(args.start_year, args.end_year)
week_list = []

# Use ThreadPoolExecutor to fetch and process data in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Map process_url function to each URL
    results = executor.map(process_url, urls)

    # Combine the results
    for result in results:
        if result:
            week_list.extend(result)

# Insert the data into the database
success = db.bulk_insert_week_details_to_db(week_list)

if success:
    print(f"Success!")
else:
    print(f"Failed!")
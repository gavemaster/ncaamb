import json
import requests

import collegebball.database as db
import collegebball.utils as utils
from datetime import datetime
from zoneinfo import ZoneInfo
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


urls = db.get_season_links_from_db(args.start_year, args.end_year)



season_details = []
season_types = []
for url in urls:
    season_details_dict = {}
    data = utils.fetch_data(url)

    if "athletes" in data:
        season_details_dict["athletes_ref"] = data["athletes"]["$ref"]
    else:
        season_details_dict["athletes_ref"] = None

    if "futures" in data:
        season_details_dict["futures_ref"] = data["futures"]["$ref"]
    else:
        season_details_dict["futures_ref"] = None

    if "leaders" in data:
        season_details_dict["leaders_ref"] = data["leaders"]["$ref"]
    else:
        season_details_dict["leaders_ref"] = None

    if "powerIndexLeaders" in data:
        season_details_dict["powerIndexLeaders_ref"] = data["powerIndexLeaders"]["$ref"]
    else:
        season_details_dict["powerIndexLeaders_ref"] = None

    if "powerIndexes" in data:
        season_details_dict["powerIndexes_ref"] = data["powerIndexes"]["$ref"]
    else:
        season_details_dict["powerIndexes_ref"] = None

    if "rankings" in data:
        season_details_dict["rankings_ref"] = data["rankings"]["$ref"]
    else:
        season_details_dict["rankings_ref"] = None

    season_details_dict["end_date"] = utils.convert_zulu_date_to_est(data["endDate"])
    season_details_dict["start_date"] = utils.convert_zulu_date_to_est(data["startDate"])
    season_details_dict["display_name"] = data["displayName"]
    season_details_dict["season"] = utils.get_first_year_from_string(data["displayName"])

    if "types" in data:
        items = data["types"]["items"]
        for type in items:
            season_types_dict = {}
            season_types_dict["type"] = type["type"]
            season_types_dict["name"] = type["name"]
            season_types_dict["start_date"] = utils.convert_zulu_date_to_est(
                type["startDate"]
            )
            season_types_dict["end_date"] = utils.convert_zulu_date_to_est(type["endDate"])
            season_types_dict["ref"] = type["$ref"]
            season_types_dict["year"] = type["year"]
            if "weeks" in type:
                season_types_dict["weeks_ref"] = type["weeks"]["$ref"]
            else:
                season_types_dict["weeks_ref"] = None
            #print(f"Type: {season_types_dict['type']}, Name: {season_types_dict['name']}, Start Date: {season_types_dict['start_date']}, End Date: {season_types_dict['end_date']}, Ref: {season_types_dict['ref']}, Year: {season_types_dict['year']}, Weeks Ref: {season_types_dict['weeks_ref']} ")
            season_types.append(season_types_dict)

    season_details.append(season_details_dict)


# Insert data into the database
success = db.bulk_insert_season_details_links(season_details)

if success:
    print("Season details successfully inserrted!")
else:
    print("Season details failed to insert!")

success = db.bulk_insert_season_types_links(season_types)

if success:
    print("Season types successfully inserrted!")
else:
    print("Season types failed to insert!")

print("Done!")

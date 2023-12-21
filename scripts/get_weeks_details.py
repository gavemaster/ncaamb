import collegebball.database as db
import json
import requests
import collegebball.utils as utils

urls = db.get_week_links_from_db(2001, 2024)

week_list = []

for url in urls:
    data = utils.fetch_data(url)
    if not data:
        continue

    # Extract the season and type from the URL
    season, type_ = utils.extract_season_and_type(url)

    weeks = data["items"]

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
        
        week_list.append(week_dict)
        


    # Insert the data into the database
success = db.bulk_insert_week_details_to_db(week_list)

if success:
    print(f"Success!")
else:
    print(f"Failed!")
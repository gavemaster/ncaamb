import requests
import collegebball.database as db
import json
from datetime import datetime
from zoneinfo import ZoneInfo


def get_team_events(team_event_link):
    team_events_list = []
    response = requests.get(team_event_link)

    if response.status_code == 200:
        team_events = json.loads(response.text)
        if "items" not in team_events:
            print("No items found")
            return None
        for item in team_events["items"]:
            team_events_list.append(item["$ref"])
        if team_events["pageIndex"] < team_events["pageCount"]:
            print("There are more pages")
            for i in range(team_events["pageIndex"] + 1, team_events["pageCount"] + 1):
                print("Getting page", i)
                response = requests.get(team_event_link + "?page=" + str(i))
                if response.status_code == 200:
                    team_events = json.loads(response.text)["items"]
                    for item in team_events:
                        team_events_list.append(item["$ref"])
                else:
                    print("Failed to retrieve data:", response.status_code)

    else:
        print("Failed to retrieve data:", response.status_code)
        team_events = None
        return None

    return team_events_list


def get_event_urls(start, end):
    team_event_links = []
    team_event_links = db.get_team_event_links_by_season_range(start, end)

    for team_event_link in team_event_links:
        team_events = get_team_events(team_event_link)
        if team_events is not None:
            team_event_links += team_events

    return team_event_links


def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


def convert_zulu_date_to_est(date):
    # Parse the string into a datetime object
    zulu_time = datetime.strptime(date, "%Y-%m-%dT%H:%MZ")

    # Set the timezone to UTC and convert to Eastern Time
    eastern_time = zulu_time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
        ZoneInfo("America/New_York")
    )
    return eastern_time.strftime("%Y-%m-%d")


def get_first_year_from_string(string):
    return string.split("-")[0]


def extract_season_and_type(url):
    # Split the URL into parts
    parts = url.split("/")

    # Find the indices for 'seasons' and 'types'
    try:
        seasons_index = parts.index("seasons")
        types_index = parts.index("types")
    except ValueError:
        # 'seasons' or 'types' not found in the URL
        return None, None

    # Extract the season and type values
    season = parts[seasons_index + 1] if seasons_index + 1 < len(parts) else None
    type_ = parts[types_index + 1] if types_index + 1 < len(parts) else None

    return season, type_


def get_event_urls_from_page(season, type, week, url):
    event_links = []
    data = fetch_data(url)
    if not data:
        return None
    events = data["items"]
    for event in events:
        event_dict = {}
        event_dict["season"] = season
        event_dict["season_type"] = type
        event_dict["week"] = week
        event_dict["event_ref"] = event["$ref"]
        event_links.append(event_dict)

    return event_links

def get_athlete_urls_from_page(season, url):
    athlete_links = []
    data = fetch_data(url)
    if not data:
        return None
    athletes = data["items"]
    for athlete in athletes:
        athlete_dict = {}
        athlete_dict["season"] = season
        athlete_dict["athlete_ref"] = athlete["$ref"]
        athlete_links.append(athlete_dict)

    return athlete_links
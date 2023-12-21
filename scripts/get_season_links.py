import json
import requests
import collegebball.database as db
import collegebball.utils as utils
base_url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/"
page = 1
all_links = []





def extract_links(data):
    links = []
    for item in data["items"]:
        link = item["$ref"]
        year = link.split("/")[-1].split("?")[0]
        links.append((year, link))
    return links


# Fetch and process all pages
while True:
    url = f"{base_url}?page={page}"
    data = utils.fetch_data(url)
    if not data or not data["items"]:
        break
    links = extract_links(data)
    all_links.extend(links)
    page += 1


# Insert data into the database
success = db.insert_season_links_to_db(all_links)

if success:
    print("Success!")
else:
    print("Failed!")
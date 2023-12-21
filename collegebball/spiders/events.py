import scrapy
import json
from datetime import datetime
from zoneinfo import ZoneInfo


from collegebball.items import EventDetailsItem, EventItem, VenueItem


class EventsSpider(scrapy.Spider):
    # spider needs to take in a list of urls
    name = "events"
    start_urls = []  # This will be populated from the script

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url["event_ref"],
                callback=self.parse_event,
                meta={
                    "season": url["season"],
                    "season_type": url["season_type"],
                    "week": url["week"],
                },
            )

    def parse_event(self, response):
        data = json.loads(response.body)
        event_item = EventItem()

        # populate the item with the data
        event_item["season"] = response.meta["season"]
        event_item["season_type"] = response.meta["season_type"]
        event_item["week"] = response.meta["week"]
        event_item["name"] = data["name"]
        event_item["shortname"] = data["shortName"]
        event_item["event_id"] = data["id"]
        # translate zulu date and time to standard eastern time
        zulu_date = data["date"]
        # Your Zulu (UTC) datetime string

        # Parse the string into a datetime object
        
        zulu_time = datetime.strptime(zulu_date, "%Y-%m-%dT%H:%MZ")

        # Set the timezone to UTC and convert to Eastern Time
        eastern_time = zulu_time.replace(tzinfo=ZoneInfo("UTC")).astimezone(
            ZoneInfo("America/New_York")
        )

        # Now you can format the datetime object to extract the date and time separately
        eastern_date = eastern_time.strftime("%Y-%m-%d")
        eastern_time_only = eastern_time.strftime("%H:%M:%S")

        event_item["event_date"] = eastern_date
        event_item["event_time"] = eastern_time_only

        event = data["competitions"][0]

        event_item["venue_id"] = event["venue"]["id"]
        
        event_details_item = EventDetailsItem()

        event_details_item["event_id"] = event_item["event_id"]

        event_details_item["neutral_site"] = event["neutralSite"]
        event_details_item["conference_game"] = event["conferenceCompetition"]
        event_details_item["division_game"] = event["divisionCompetition"]
        event_details_item["event_ref"] = event["$ref"]

        if "details" in event:
            event_details_item["event_details_ref"] = event["details"]["$ref"]
        else:
            event_details_item["event_details_ref"] = None

        if "odds" in event:
            event_details_item["odds_ref"] = event["odds"]["$ref"]
        else:
            event_details_item["odds_ref"] = None

        if "status" in event:
            event_details_item["status_ref"] = event["status"]["$ref"]
        else:
            event_details_item["status_ref"] = None

        if "venue" in event:
            venue_item = VenueItem()
            venue = event["venue"]
            venue_item["venue_id"] = venue["id"]
            venue_item["name"] = venue["fullName"]
            venue_item["city"] = venue["address"]["city"]
            venue_item["state"] = venue["address"]["state"]
            venue_item["capacity"] = venue["capacity"]
            venue_item["indoor"] = venue["indoor"]
            venue_item["venue_ref"] = venue["$ref"]
            yield venue_item

        # get the teams
        home_team = event["competitors"][0]
        away_team = event["competitors"][1]

        event_item["home_team_college_id"] = home_team["id"]
        event_item["away_team_college_id"] = away_team["id"]
        
        if "score" in home_team:
            home_score_link = home_team["score"]["$ref"]
        else:
            home_score_link = None

        if "score" in away_team:
            away_score_link = away_team["score"]["$ref"]
        else:
            away_score_link = None

        if "curatedRank" in home_team:
            event_details_item["home_team_rank"] = home_team["curatedRank"]["current"]
        else:
            event_details_item["home_team_rank"] = None

        if "curatedRank" in away_team:
            event_details_item["away_team_rank"] = away_team["curatedRank"]["current"]
        else:
            event_details_item["away_team_rank"] = None

        if "statistics" in home_team:
            event_details_item["home_team_stats_ref"] = home_team["statistics"]["$ref"]
        else:
            event_details_item["home_team_stats_ref"] = None

        if "statistics" in away_team:
            event_details_item["away_team_stats_ref"] = away_team["statistics"]["$ref"]
        else:
            event_details_item["away_team_stats_ref"] = None

        if "roster" in home_team:
            event_details_item["home_team_roster_ref"] = home_team["roster"]["$ref"]
        else:
            event_details_item["home_team_roster_ref"] = None

        if "roster" in away_team:
            event_details_item["away_team_roster_ref"] = away_team["roster"]["$ref"]
        else:
            event_details_item["away_team_roster_ref"] = None

        if "winnner" in home_team:
            event_details_item["home_team_win"] = home_team["winner"]
        else:
            event_details_item["home_team_win"] = None
        
        if "winner" in away_team:
            event_details_item["away_team_win"] = away_team["winner"]
        else:
            event_details_item["away_team_win"] = None

            
        if "record" in home_team:
            event_details_item["home_team_record_ref"] = home_team["record"]["$ref"]
        else:
            event_details_item["home_team_record_ref"] = None

        if "record" in away_team:
            event_details_item["away_team_record_ref"] = away_team["record"]["$ref"]
        else:
            event_details_item["away_team_record_ref"] = None

        if event_details_item["status_ref"] is not None:
            yield scrapy.Request(
                url=event_details_item["status_ref"],
                callback=self.parse_status,
                meta={
                    "event_details_item": event_details_item,
                    "event_item": event_item,
                    "home_score_link": home_score_link,
                    "away_score_link": away_score_link,
                },
            )

        elif home_score_link is not None and away_score_link is not None:
            yield scrapy.Request(
                url=home_score_link,
                callback=self.parse_score,
                meta={
                    "event_item": event_item,
                    "event_details_item": event_details_item,
                    "team": "home",
                    "away_score_link": away_score_link,
                },
            )
        else:
            yield event_item
            yield event_details_item

    def parse_status(self, response):
        data = json.loads(response.body)
        event_details_item = response.meta["event_details_item"]
        event_item = response.meta["event_item"]
        home_score_link = response.meta["home_score_link"]
        away_score_link = response.meta["away_score_link"]
        event_details_item["status"] = data["type"]["name"]

        if home_score_link is not None and away_score_link is not None:
            yield scrapy.Request(
                url=home_score_link,
                callback=self.parse_score,
                meta={
                    "event_item": event_item,
                    "event_details_item": event_details_item,
                    "team": "home",
                    "away_score_link": away_score_link,
                },
            )
        else:
            yield event_item
            yield event_details_item

    def parse_score(self, response):
        data = json.loads(response.body)
        team = response.meta["team"]
        event_details_item = response.meta["event_details_item"]
        event_item = response.meta["event_item"]
        
        if team == "home":
            event_item["home_team_score"] = data["value"]
            yield scrapy.Request(
                url=response.meta["away_score_link"],
                callback=self.parse_score,
                meta={"event_item": event_item, "team": "away", "event_details_item": event_details_item},
            )
        else:
            event_item["away_team_score"] = data["value"]
            yield event_item
            yield event_details_item

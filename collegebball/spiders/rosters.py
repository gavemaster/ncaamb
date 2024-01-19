import scrapy
import json
from collegebball.items import RosterItem


class RostersSpider(scrapy.Spider):
    name = "rosters"
    start_urls = []  # This will be populated from the script

    def start_requests(self):
        for url in self.start_urls:
            if url["home_team_roster_url"] is not None:
            
                yield scrapy.Request(
                    url=url["home_team_roster_url"],
                    callback=self.parse_roster,
                    meta={
                        "season": url["season"],
                        "home/away": "home",
                        "home_team": url["home_team"],
                        "away_team": url["away_team"],
                        "event_id": url["event_id"],
                        "home_team_roster_url": url["home_team_roster_url"],
                        "away_team_roster_url": url["away_team_roster_url"],
                    },
                )
            elif url["home_team_roster_url"] is None and url["away_team_roster_url"] is not None:
                yield scrapy.Request(
                    url=url["away_team_roster_url"],
                    callback=self.parse_roster,
                    meta={
                        "season": url["season"],
                        "home/away": "away",
                        "home_team": url["home_team"],
                        "away_team": url["away_team"],
                        "event_id": url["event_id"],
                        "home_team_roster_url": url["home_team_roster_url"],
                        "away_team_roster_url": url["away_team_roster_url"],
                    },
                )
            else:
                continue


    def parse_roster(self, response):
        season = response.meta["season"]
        home_away = response.meta["home/away"]
        
        event_id = response.meta["event_id"]
        roster = json.loads(response.body)["entries"]

        for player in roster:
            roster_item = RosterItem()
            if home_away == "home":
                roster_item["team_id"] = response.meta['home_team']
            else:
                roster_item["team_id"] = response.meta['away_team']
            roster_item["season"] = season
            roster_item["event_id"] = event_id

            roster_item["athlete_id"] = player["playerId"]

            if "starter" in player:
                roster_item["starter"] = player["starter"]
            else:
                roster_item["starter"] = False

            if "didNotPlay" in player:
                roster_item["did_not_play"] = player["didNotPlay"]
            else:
                roster_item["did_not_play"] = False

            if "ejected" in player:
                roster_item["ejected"] = player["ejected"]
            else:
                roster_item["ejected"] = False

            if "statistics" in player:
                roster_item["stats_ref"] = player["statistics"]["$ref"]
            else:
                roster_item["stats_ref"] = None

            if "athlete" in player:
                roster_item["athlete_ref"] = player["athlete"]["$ref"]
            else:
                roster_item["athlete_ref"] = None
            yield roster_item

        if home_away == "home" and response.meta["away_team_roster_url"] is not None:
            yield scrapy.Request(
                url=response.meta["away_team_roster_url"],
                callback=self.parse_roster,
                meta={
                    "season": season,
                    "home/away": "away",
                    "home_team": response.meta["home_team"],
                    "away_team": response.meta["away_team"],
                    "event_id": event_id,
                    "home_team_roster_url": response.meta["home_team_roster_url"],
                    "away_team_roster_url": response.meta["away_team_roster_url"],
                },
            )

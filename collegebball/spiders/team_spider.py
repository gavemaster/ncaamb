from collegebball.items import (
    ConferenceDetailsItem,
    ConferenceItem,
    TeamItem,
    TeamOutcomesItem,
    CollegeItem,
)
import scrapy
import json
import logging


class TeamSpiderSpider(scrapy.Spider):
    name = "team_spider"

    def start_requests(self):
        self.logger.info("Starting to crawl URLs")
        for i in range(2021, 2024):
            url = f"http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/{i}/teams"
            self.logger.debug(f"Generating request for URL: {url}")
            yield scrapy.Request(
                url=url, callback=self.parse_team_landing_page, meta={"season": i}
            )

    def parse_team_landing_page(self, response):
        self.logger.debug(f"Processing response from {response.url}")
        season = response.meta["season"]
        data = json.loads(response.body)
        page_count = data["pageCount"]
        team_landing_pages = []
        for i in range(1, page_count + 1):
            team_landing_pages.append(f"{response.url}?page={i}")

        for url in team_landing_pages:
            yield scrapy.Request(
                url=url, callback=self.parse_team_page, meta={"season": season}
            )

    def parse_team_page(self, response):
        self.logger.debug(f"Processing team page response from {response.url}")
        data = json.loads(response.body)["items"]
        season = response.meta["season"]
        for team_url in data:
            yield scrapy.Request(
                url=team_url["$ref"], callback=self.parse_team, meta={"season": season}
            )

    def parse_team(self, response):
        self.logger.debug(f"Parsing team data from {response.url}")

        data = json.loads(response.body)
        team_links = {}
        college_item = CollegeItem()
        team_item = TeamItem()

        team_outcomes_item = TeamOutcomesItem()
        team_item["team_name"] = data["displayName"]
        team_item["college_espn_id"] = data["id"]
        team_item["team_location"] = data["location"]

        college_item["team_name"] = team_item["team_name"]
        college_item["college_espn_id"] = team_item["college_espn_id"]
        college_item["team_location"] = team_item["team_location"]

        yield college_item

        team_item["team_abbr"] = data["abbreviation"]
        team_item["season"] = response.meta["season"]
        # set defaults
        team_outcomes_item["college_espn_id"] = team_item["college_espn_id"]
        team_outcomes_item["season"] = team_item["season"]
        team_outcomes_item["ats_overall_wins"] = -1
        team_outcomes_item["ats_overall_losses"] = -1
        team_outcomes_item["ats_overall_pushes"] = -1
        team_outcomes_item["ats_favorite_wins"] = -1
        team_outcomes_item["ats_favorite_losses"] = -1
        team_outcomes_item["ats_favorite_pushes"] = -1
        team_outcomes_item["ats_underdog_wins"] = -1
        team_outcomes_item["ats_underdog_losses"] = -1
        team_outcomes_item["ats_underdog_pushes"] = -1
        team_outcomes_item["ats_away_wins"] = -1
        team_outcomes_item["ats_away_losses"] = -1
        team_outcomes_item["ats_away_pushes"] = -1
        team_outcomes_item["ats_home_wins"] = -1
        team_outcomes_item["ats_home_losses"] = -1
        team_outcomes_item["ats_home_pushes"] = -1
        team_outcomes_item["ats_away_favorite_wins"] = -1
        team_outcomes_item["ats_away_favorite_losses"] = -1
        team_outcomes_item["ats_away_favorite_pushes"] = -1
        team_outcomes_item["ats_away_underdog_wins"] = -1
        team_outcomes_item["ats_away_underdog_losses"] = -1
        team_outcomes_item["ats_away_underdog_pushes"] = -1
        team_outcomes_item["ats_home_favorite_wins"] = -1
        team_outcomes_item["ats_home_favorite_losses"] = -1
        team_outcomes_item["ats_home_favorite_pushes"] = -1
        team_outcomes_item["ats_home_underdog_wins"] = -1
        team_outcomes_item["ats_home_underdog_losses"] = -1
        team_outcomes_item["ats_home_underdog_pushes"] = -1
        team_outcomes_item["team_ovr_wins"] = -1
        team_outcomes_item["team_ovr_losses"] = -1
        team_outcomes_item["team_ovr_ot_losses"] = -1
        team_outcomes_item["team_ovr_ot_wins"] = -1
        team_outcomes_item["team_ovr_point_diff"] = -1
        team_outcomes_item["team_ovr_avg_points"] = -1
        team_outcomes_item["team_ovr_avg_points_allowed"] = -1
        team_outcomes_item["team_ovr_division_win_percentage"] = -1
        team_outcomes_item["team_ovr_games_played"] = -1
        team_outcomes_item["team_home_wins"] = -1
        team_outcomes_item["team_home_losses"] = -1
        team_outcomes_item["team_home_ot_losses"] = -1
        team_outcomes_item["team_home_ot_wins"] = -1
        team_outcomes_item["team_home_point_diff"] = -1
        team_outcomes_item["team_home_avg_points"] = -1
        team_outcomes_item["team_home_avg_points_allowed"] = -1
        team_outcomes_item["team_home_division_win_percentage"] = -1
        team_outcomes_item["team_home_games_played"] = -1
        team_outcomes_item["team_away_wins"] = -1
        team_outcomes_item["team_away_losses"] = -1
        team_outcomes_item["team_away_ot_losses"] = -1
        team_outcomes_item["team_away_ot_wins"] = -1
        team_outcomes_item["team_away_point_diff"] = -1
        team_outcomes_item["team_away_avg_points"] = -1
        team_outcomes_item["team_away_avg_points_allowed"] = -1
        team_outcomes_item["team_away_division_win_percentage"] = -1
        team_outcomes_item["team_away_games_played"] = -1
        team_outcomes_item["team_versus_ap_top_25_wins"] = -1
        team_outcomes_item["team_versus_ap_top_25_losses"] = -1
        team_outcomes_item["team_versus_ap_top_25_ot_losses"] = -1
        team_outcomes_item["team_versus_ap_top_25_ot_wins"] = -1
        team_outcomes_item["team_versus_ap_top_25_point_diff"] = -1
        team_outcomes_item["team_versus_ap_top_25_avg_points"] = -1
        team_outcomes_item["team_versus_ap_top_25_avg_points_allowed"] = -1
        team_outcomes_item["team_versus_ap_top_25_games_played"] = -1
        team_outcomes_item["team_versus_conf_wins"] = -1
        team_outcomes_item["team_versus_conf_losses"] = -1
        team_outcomes_item["team_versus_conf_ot_losses"] = -1
        team_outcomes_item["team_versus_conf_ot_wins"] = -1
        team_outcomes_item["team_versus_conf_point_diff"] = -1
        team_outcomes_item["team_versus_conf_avg_points"] = -1
        team_outcomes_item["team_versus_conf_avg_points_allowed"] = -1
        team_outcomes_item["team_versus_conf_division_win_percentage"] = -1
        team_outcomes_item["team_versus_conf_games_played"] = -1

        if "againstTheSpread" in data:
            team_links["ats_link"] = data["againstTheSpread"]["$ref"]
        elif "againstTheSpreadRecords" in data:
            team_links["ats_link"] = data["againstTheSpreadRecords"]["$ref"]
        else:
            team_links["ats_link"] = None

        if "events" in data:
            team_item["team_events_ref"] = data["events"]["$ref"]
        else:
            team_item["team_events_ref"] = None

        if "groups" in data:
            team_links["conference"] = data["groups"]["$ref"]
        else:
            team_links["conference"] = None
            team_item["team_conference"] = None
            team_item["team_conference_shortname"] = None

        if "ranks" in data:
            team_links["ranks"] = data["ranks"]["$ref"]
        else:
            team_links["ranks"] = None

        if "record" in data:
            team_links["record"] = data["record"]["$ref"]
        else:
            team_links["record"] = None

        if team_links["conference"] != None:
            yield scrapy.Request(
                url=team_links["conference"],
                callback=self.parse_conference,
                meta={
                    "team_item": team_item,
                    "team_links": team_links,
                    "team_outcomes_item": team_outcomes_item,
                },
            )
        else:
            yield team_item
            
            if team_links["ats_link"] != None:
                yield scrapy.Request(
                    url=team_links["ats_link"],
                    callback=self.parse_ats,
                    meta={
                        "team_outcomes_item": team_outcomes_item,
                        "team_links": team_links,
                    },
                )
            elif team_links["record"] != None:
                yield scrapy.Request(
                    url=team_links["record"],
                    callback=self.parse_record,
                    meta={
                        "team_outcomes_item": team_outcomes_item,
                        "team_links": team_links,
                    },
                )
            else:
                yield team_outcomes_item

    def parse_ats(self, response):
        self.logger.debug(f"Parsing ATS data from {response.url}")
        data = json.loads(response.body)["items"]
        team_outcomes_item = response.meta["team_outcomes_item"]
        team_links = response.meta["team_links"]

        if data:
            for record in data:
                record_type = record["type"]
                record_name = record_type["name"]
                if record_name == "atsOverall":
                    team_outcomes_item["ats_overall_wins"] = record["wins"]
                    team_outcomes_item["ats_overall_losses"] = record["losses"]
                    team_outcomes_item["ats_overall_pushes"] = record["pushes"]
                elif record_name == "atsFavorite":
                    team_outcomes_item["ats_favorite_wins"] = record["wins"]
                    team_outcomes_item["ats_favorite_losses"] = record["losses"]
                    team_outcomes_item["ats_favorite_pushes"] = record["pushes"]
                elif record_name == "atsUnderdog":
                    team_outcomes_item["ats_underdog_wins"] = record["wins"]
                    team_outcomes_item["ats_underdog_losses"] = record["losses"]
                    team_outcomes_item["ats_underdog_pushes"] = record["pushes"]
                elif record_name == "atsAway":
                    team_outcomes_item["ats_away_wins"] = record["wins"]
                    team_outcomes_item["ats_away_losses"] = record["losses"]
                    team_outcomes_item["ats_away_pushes"] = record["pushes"]
                elif record_name == "atsHome":
                    team_outcomes_item["ats_home_wins"] = record["wins"]
                    team_outcomes_item["ats_home_losses"] = record["losses"]
                    team_outcomes_item["ats_home_pushes"] = record["pushes"]
                elif record_name == "atsAwayFavorite":
                    team_outcomes_item["ats_away_favorite_wins"] = record["wins"]
                    team_outcomes_item["ats_away_favorite_losses"] = record["losses"]
                    team_outcomes_item["ats_away_favorite_pushes"] = record["pushes"]
                elif record_name == "atsAwayUnderdog":
                    team_outcomes_item["ats_away_underdog_wins"] = record["wins"]
                    team_outcomes_item["ats_away_underdog_losses"] = record["losses"]
                    team_outcomes_item["ats_away_underdog_pushes"] = record["pushes"]
                elif record_name == "atsHomeFavorite":
                    team_outcomes_item["ats_home_favorite_wins"] = record["wins"]
                    team_outcomes_item["ats_home_favorite_losses"] = record["losses"]
                    team_outcomes_item["ats_home_favorite_pushes"] = record["pushes"]
                elif record_name == "atsHomeUnderdog":
                    team_outcomes_item["ats_home_underdog_wins"] = record["wins"]
                    team_outcomes_item["ats_home_underdog_losses"] = record["losses"]
                    team_outcomes_item["ats_home_underdog_pushes"] = record["pushes"]
                else:
                    print("ERROR: record_name not found")
                    print(record_name)
                    print(record)
                    print("ERROR: record_name not found")
        else:
            print("no ats data found")

        if team_links["record"] != None:
            yield scrapy.Request(
                url=team_links["record"],
                callback=self.parse_record,
                meta={
                    "team_outcomes_item": team_outcomes_item,
                    "team_links": team_links,
                },
            )
        else:
            yield team_outcomes_item

    def parse_conference(self, response):
        self.logger.debug(f"Parsing conference data from {response.url}")
        data = json.loads(response.body)
        team_links = response.meta["team_links"]
        team_item = response.meta["team_item"]
        team_outcomes_item = response.meta["team_outcomes_item"]
        conf_item = ConferenceItem()
        conf_detail_item = ConferenceDetailsItem()

        conf_item["name"] = data["name"]
        conf_item["conference_id"] = data["id"]

        conf_detail_item["name"] = conf_item["name"]
        conf_detail_item["conference_id"] = conf_item["conference_id"]

        team_item["team_conference"] = conf_item["conference_id"]

        yield conf_item

        conf_detail_item["season"] = team_item["season"]

        conf_detail_item["short_name"] = data["shortName"]

        if "standings" in data:
            conf_detail_item["conference_standings_ref"] = data["standings"]["$ref"]
        else:
            conf_detail_item["conference_standings_ref"] = None

        if "teams" in data:
            conf_detail_item["conference_teams_ref"] = data["teams"]["$ref"]
        else:
            conf_detail_item["conference_teams_ref"] = None

        yield conf_detail_item

        yield team_item
        
        
        if team_links["ats_link"] != None:
            yield scrapy.Request(
                url=team_links["ats_link"],
                callback=self.parse_ats,
                meta={
                    "team_outcomes_item": team_outcomes_item,
                    "team_links": team_links,
                },
            )
        elif team_links["record"] != None:
            yield scrapy.Request(
                url=team_links["record"],
                callback=self.parse_record,
                meta={
                    "team_outcomes_item": team_outcomes_item,
                    "team_links": team_links,
                },
            )
        else:
            yield team_outcomes_item

    def parse_record(self, response):
        self.logger.debug(f"Parsing record data from {response.url}")
        data = json.loads(response.body)["items"]
        team_outcomes_item = response.meta["team_outcomes_item"]

        # set defaults

        for record in data:
            record_type = record["name"]
            if record_type == "overall":
                stats = record["stats"]
                for stat in stats:
                    stat_name = stat["name"]
                    if stat_name == "wins":
                        team_outcomes_item["team_ovr_wins"] = stat["displayValue"]
                    elif stat_name == "losses":
                        team_outcomes_item["team_ovr_losses"] = stat["displayValue"]
                    elif stat_name == "OTLosses":
                        team_outcomes_item["team_ovr_ot_losses"] = stat["displayValue"]
                    elif stat_name == "OTWins":
                        team_outcomes_item["team_ovr_ot_wins"] = stat["displayValue"]
                    elif stat_name == "differential":
                        team_outcomes_item["team_ovr_point_diff"] = stat["displayValue"]
                    elif stat_name == "avgPointsFor":
                        team_outcomes_item["team_ovr_avg_points"] = stat["displayValue"]
                    elif stat_name == "avgPointsAgainst":
                        team_outcomes_item["team_ovr_avg_points_allowed"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "divisionWinPercent":
                        team_outcomes_item["team_ovr_division_win_percentage"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "gamesPlayed":
                        team_outcomes_item["team_ovr_games_played"] = stat[
                            "displayValue"
                        ]
            elif record_type == "Home":
                stats = record["stats"]
                for stat in stats:
                    stat_name = stat["name"]
                    if stat_name == "wins":
                        team_outcomes_item["team_home_wins"] = stat["displayValue"]
                    elif stat_name == "losses":
                        team_outcomes_item["team_home_losses"] = stat["displayValue"]
                    elif stat_name == "OTLosses":
                        team_outcomes_item["team_home_ot_losses"] = stat["displayValue"]
                    elif stat_name == "OTWins":
                        team_outcomes_item["team_home_ot_wins"] = stat["displayValue"]
                    elif stat_name == "differential":
                        team_outcomes_item["team_home_point_diff"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsFor":
                        team_outcomes_item["team_home_avg_points"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsAgainst":
                        team_outcomes_item["team_home_avg_points_allowed"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "divisionWinPercent":
                        team_outcomes_item["team_home_division_win_percentage"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "gamesPlayed":
                        team_outcomes_item["team_home_games_played"] = stat[
                            "displayValue"
                        ]
            elif record_type == "Road":
                stats = record["stats"]
                for stat in stats:
                    stat_name = stat["name"]
                    if stat_name == "wins":
                        team_outcomes_item["team_away_wins"] = stat["displayValue"]
                    elif stat_name == "losses":
                        team_outcomes_item["team_away_losses"] = stat["displayValue"]
                    elif stat_name == "OTLosses":
                        team_outcomes_item["team_away_ot_losses"] = stat["displayValue"]
                    elif stat_name == "OTWins":
                        team_outcomes_item["team_away_ot_wins"] = stat["displayValue"]
                    elif stat_name == "differential":
                        team_outcomes_item["team_away_point_diff"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsFor":
                        team_outcomes_item["team_away_avg_points"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsAgainst":
                        team_outcomes_item["team_away_avg_points_allowed"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "divisionWinPercent":
                        team_outcomes_item["team_away_division_win_percentage"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "gamesPlayed":
                        team_outcomes_item["team_away_games_played"] = stat[
                            "displayValue"
                        ]
            elif record_type == "vs AP Top 25":
                stats = record["stats"]
                for stat in stats:
                    stat_name = stat["name"]
                    if stat_name == "wins":
                        team_outcomes_item["team_versus_ap_top_25_wins"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "losses":
                        team_outcomes_item["team_versus_ap_top_25_losses"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "OTLosses":
                        team_outcomes_item["team_versus_ap_top_25_ot_losses"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "OTWins":
                        team_outcomes_item["team_versus_ap_top_25_ot_wins"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "differential":
                        team_outcomes_item["team_versus_ap_top_25_point_diff"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsFor":
                        team_outcomes_item["team_versus_ap_top_25_avg_points"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsAgainst":
                        team_outcomes_item[
                            "team_versus_ap_top_25_avg_points_allowed"
                        ] = stat["displayValue"]
                    elif stat_name == "gamesPlayed":
                        team_outcomes_item["team_versus_ap_top_25_games_played"] = stat[
                            "displayValue"
                        ]
            elif record_type == "vs. Conf.":
                stats = record["stats"]
                for stat in stats:
                    stat_name = stat["name"]
                    if stat_name == "wins":
                        team_outcomes_item["team_versus_conf_wins"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "losses":
                        team_outcomes_item["team_versus_conf_losses"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "OTLosses":
                        team_outcomes_item["team_versus_conf_ot_losses"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "OTWins":
                        team_outcomes_item["team_versus_conf_ot_wins"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "differential":
                        team_outcomes_item["team_versus_conf_point_diff"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsFor":
                        team_outcomes_item["team_versus_conf_avg_points"] = stat[
                            "displayValue"
                        ]
                    elif stat_name == "avgPointsAgainst":
                        team_outcomes_item[
                            "team_versus_conf_avg_points_allowed"
                        ] = stat["displayValue"]

            yield team_outcomes_item

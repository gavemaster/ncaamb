from collegebball.items import (

    ConferenceItem,
    TeamItem,

)
import scrapy
import json
import logging
import collegebball.utils as utils

class TeamSpiderSpider(scrapy.Spider):
    name = "team_spider"
    start_urls= []
    def start_requests(self):
        self.logger.info("Starting to crawl URLs")
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_team)

    

    def parse_team(self, response):
        self.logger.debug(f"Parsing team data from {response.url}")

        data = json.loads(response.body)


        team_item = TeamItem()


        team_item["team_name"] = data["displayName"]
        team_item["college_espn_id"] = data["id"]
        team_item["team_location"] = data["location"]

        team_item["team_name"] = team_item["team_name"]
        team_item["college_espn_id"] = team_item["college_espn_id"]
        team_item["team_location"] = team_item["team_location"]

   

        if "abbreviation" in data:
            team_item["team_abbr"] = data["abbreviation"]
        else:
            team_item["team_abbr"] = None
        
        
        
        if "againstTheSpread" in data:
            team_item["ats_ref"] = data["againstTheSpread"]["$ref"]
        elif "againstTheSpreadRecords" in data:
            team_item["ats_ref"] = data["againstTheSpreadRecords"]["$ref"]
        else:
            team_item["ats_ref"] = None

        if "events" in data:
            team_item["team_events_ref"] = data["events"]["$ref"]
        else:
            team_item["team_events_ref"] = None

        if "groups" in data:
            team_item["conference_ref"] = data["groups"]["$ref"]
        else:
            team_item["conference_ref"] = None
            team_item["team_conference"] = None

            

        if "ranks" in data:
            team_item["ranks_ref"] = data["ranks"]["$ref"]
        else:
            team_item["ranks_ref"] = None

        if "record" in data:
            team_item["record_ref"] = data["record"]["$ref"]
        else:
            team_item["record_ref"] = None

        
        if team_item["conference_ref"] != None:
            team_item["team_conference"] = utils.extract_id_from_url(team_item["conference_ref"])
        else:
            team_item["team_conference"] = None


        team_item["season"] = utils.extract_year_from_url(response.url)

        yield team_item
        
                    
        
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
            pass

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
        
        conf_item["name"] = data["name"]
        conf_item["conference_id"] = data["id"]

        team_item["team_conference"] = conf_item["conference_id"]

        

        conf_item["season"] = team_item["season"]

        conf_item["short_name"] = data["shortName"]

        if "standings" in data:
            conf_item["conference_standings_ref"] = data["standings"]["$ref"]
        else:
            conf_item["conference_standings_ref"] = None

        if "teams" in data:
            conf_item["conference_teams_ref"] = data["teams"]["$ref"]
        else:
            conf_item["conference_teams_ref"] = None

        yield conf_item

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

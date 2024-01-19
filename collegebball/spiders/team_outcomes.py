import scrapy
import json

from collegebball.items import TeamOutcomesItem

class TeamOutcomesSpider(scrapy.Spider):
    name = "team_outcomes"
    start_urls= []
    def start_requests(self):
        self.logger.info("Starting to crawl URLs")
        for url in self.start_urls:
            yield scrapy.Request(url=url["ats_link"], callback=self.parse, meta={"team_id": url["team_id"], "season": url["season"], "record_link": url["record_link"]})

    
    def parse(self, response):
        self.logger.debug(f"Parsing ATS data from {response.url}")
        data = json.loads(response.body)["items"]
        team_outcomes_item = TeamOutcomesItem()
        team_outcomes_item["college_espn_id"] = response.meta["team_id"]
        team_outcomes_item["season"] = response.meta["season"]
        #SET ALL TEAM_OUTCOMES_ITEM FIELDS TO 0
        team_outcomes_item["ats_overall_wins"] = 0
        team_outcomes_item["ats_overall_losses"] = 0
        team_outcomes_item["ats_overall_pushes"] = 0
        team_outcomes_item["ats_favorite_wins"] = 0
        team_outcomes_item["ats_favorite_losses"] = 0
        team_outcomes_item["ats_favorite_pushes"] = 0
        team_outcomes_item["ats_underdog_wins"] = 0
        team_outcomes_item["ats_underdog_losses"] = 0
        team_outcomes_item["ats_underdog_pushes"] = 0
        team_outcomes_item["ats_away_wins"] = 0
        team_outcomes_item["ats_away_losses"] = 0
        team_outcomes_item["ats_away_pushes"] = 0
        team_outcomes_item["ats_home_wins"] = 0
        team_outcomes_item["ats_home_losses"] = 0
        team_outcomes_item["ats_home_pushes"] = 0
        team_outcomes_item["ats_away_favorite_wins"] = 0
        team_outcomes_item["ats_away_favorite_losses"] = 0
        team_outcomes_item["ats_away_favorite_pushes"] = 0
        team_outcomes_item["ats_away_underdog_wins"] = 0
        team_outcomes_item["ats_away_underdog_losses"] = 0
        team_outcomes_item["ats_away_underdog_pushes"] = 0
        team_outcomes_item["ats_home_favorite_wins"] = 0
        team_outcomes_item["ats_home_favorite_losses"] = 0
        team_outcomes_item["ats_home_favorite_pushes"] = 0
        team_outcomes_item["ats_home_underdog_wins"] = 0
        team_outcomes_item["ats_home_underdog_losses"] = 0
        team_outcomes_item["ats_home_underdog_pushes"] = 0
        team_outcomes_item["team_ovr_wins"] = 0
        team_outcomes_item["team_ovr_losses"] = 0
        team_outcomes_item["team_ovr_ot_losses"] = 0
        team_outcomes_item["team_ovr_ot_wins"] = 0
        team_outcomes_item["team_ovr_point_diff"] = 0
        team_outcomes_item["team_ovr_avg_points"] = 0
        team_outcomes_item["team_ovr_avg_points_allowed"] = 0
        team_outcomes_item["team_ovr_division_win_percentage"] = 0
        team_outcomes_item["team_ovr_games_played"] = 0
        team_outcomes_item["team_home_wins"] = 0
        team_outcomes_item["team_home_losses"] = 0
        team_outcomes_item["team_home_ot_losses"] = 0
        team_outcomes_item["team_home_ot_wins"] = 0
        team_outcomes_item["team_home_point_diff"] = 0
        team_outcomes_item["team_home_avg_points"] = 0
        team_outcomes_item["team_home_avg_points_allowed"] = 0
        team_outcomes_item["team_home_division_win_percentage"] = 0
        team_outcomes_item["team_home_games_played"] = 0
        team_outcomes_item["team_away_wins"] = 0
        team_outcomes_item["team_away_losses"] = 0
        team_outcomes_item["team_away_ot_losses"] = 0
        team_outcomes_item["team_away_ot_wins"] = 0
        team_outcomes_item["team_away_point_diff"] = 0
        team_outcomes_item["team_away_avg_points"] = 0
        team_outcomes_item["team_away_avg_points_allowed"] = 0
        team_outcomes_item["team_away_division_win_percentage"] = 0
        team_outcomes_item["team_away_games_played"] = 0
        team_outcomes_item["team_versus_ap_top_25_wins"] = 0
        team_outcomes_item["team_versus_ap_top_25_losses"] = 0
        team_outcomes_item["team_versus_ap_top_25_ot_losses"] = 0
        team_outcomes_item["team_versus_ap_top_25_ot_wins"] = 0
        team_outcomes_item["team_versus_ap_top_25_point_diff"] = 0
        team_outcomes_item["team_versus_ap_top_25_avg_points"] = 0
        team_outcomes_item["team_versus_ap_top_25_avg_points_allowed"] = 0
        team_outcomes_item["team_versus_ap_top_25_games_played"] = 0
        team_outcomes_item["team_versus_conf_wins"] = 0
        team_outcomes_item["team_versus_conf_losses"] = 0
        team_outcomes_item["team_versus_conf_ot_losses"] = 0
        team_outcomes_item["team_versus_conf_ot_wins"] = 0
        team_outcomes_item["team_versus_conf_point_diff"] = 0
        team_outcomes_item["team_versus_conf_avg_points"] = 0
        team_outcomes_item["team_versus_conf_avg_points_allowed"] = 0
        team_outcomes_item["team_versus_conf_games_played"] = 0

            
        
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
                    continue
        else:
            pass

        yield scrapy.Request(url=response.meta["record_link"], callback=self.parse_record, meta={"team_outcomes_item": team_outcomes_item})


    def parse_record(self, response):
        self.logger.debug(f"Parsing record data from {response.url}")
        data = json.loads(response.body)["items"]
        team_outcomes_item = response.meta["team_outcomes_item"]


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
                    elif stat_name == "gamesPlayed":
                        team_outcomes_item["team_versus_conf_games_played"] = stat[
                            "displayValue"
                        ]

            yield team_outcomes_item
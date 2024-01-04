import scrapy
import json
from collegebball.items import PlayerStatsItem


class PlayerStatsSpider(scrapy.Spider):
    name = "player_stats"
    start_urls = []

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url["stats_ref"], callback=self.parse_player_stats, meta={"team_id": url["team_id"], "athlete_id": url["athlete_id"], 
                                                                                  "event_id": url["event_id"], "season": url["season"]})

    def parse_player_stats(self, response):
        stat_cats = json.loads(response.body)["splits"]["categories"]

        for cat in stat_cats:
            if cat["name"] == "defensive":
                def_stats = cat["stats"]
            elif cat["name"] == "offensive":
                off_stats = cat["stats"]
            elif cat["name"] == "general":
                gen_stats = cat["stats"]

        stats_item = PlayerStatsItem()

        stats_item["team_id"] = response.meta["team_id"]
        stats_item["athlete_id"] = response.meta["athlete_id"]
        stats_item["event_id"] = response.meta["event_id"]
        stats_item["season"] = response.meta["season"]

        for stat in def_stats:
            if stat["name"] == "blocks":
                stats_item["blocks"] = stat["value"]
            elif stat["name"] == "defensiveRebounds":
                stats_item["def_rebounds"] = stat["value"]
            elif stat["name"] == "steals":
                stats_item["steals"] = stat["value"]
            elif stat["name"] == "turnoverPoints":
                stats_item["points_off_turnovers"] = stat["value"]

        for stat in gen_stats:
            if stat["name"] == "flagrantFouls":
                stats_item["flagrant_fouls"] = stat["value"]
            elif stat["name"] == "fouls":
                stats_item["fouls"] = stat["value"]
            elif stat["name"] == "ejections":
                stats_item["ejections"] = stat["value"]
            elif stat["name"] == "technicalFouls":
                stats_item["technical_fouls"] = stat["value"]
            elif stat["name"] == "rebounds":
                stats_item["tot_rebounds"] = stat["value"]
            elif stat["name"] == "minutes":
                stats_item["total_minutes"] = stat["value"]
            elif stat["name"] == "fantasyRating":
                stats_item["fantasy_rating"] = stat["value"]
            elif stat["name"] == "plusMinus":
                stats_item["plus_minus"] = stat["value"]
            elif stat["name"] == "assistTurnoverRatio":
                stats_item["assist_turnover_ratio"] = stat["value"]
            elif stat["name"] == "stealFoulRatio":
                stats_item["steal_foul_ratio"] = stat["value"]
            elif stat["name"] == "blockFoulRatio":
                stats_item["blocks_foul_ratio"] = stat["value"]
            elif stat["name"] == "stealTurnoverRatio":
                stats_item["steal_turnover_ratio"] = stat["value"]
            elif stat["name"] == "totalRebounds" and stats_item["tot_rebounds"] is None:
                stats_item["tot_rebounds"] = stat["value"]
            elif stat["name"] == "totalTechnicalFouls" and stats_item["technical_fouls"] is None:
                stats_item["technical_fouls"] = stat["value"]
            elif stat["name"] == "gamesPlayed":
                stats_item["games_played"] = stat["value"]
            elif stat["name"] == "gamesStarted":
                stats_item["games_started"] = stat["value"]
            elif stat["name"] == "doubleDouble":
                stats_item["double_double"] = stat["value"]
            elif stat["name"] == "tripleDouble":
                stats_item["triple_double"] = stat["value"]
            

        for stat in off_stats:
            if stat["name"] == "assists":
                stats_item["assists"] = stat["value"]
            elif stat["name"] == "fieldGoalsAttempted":
                stats_item["fgs_attempted"] = stat["value"]
            elif stat["name"] == "fieldGoalsMade":
                stats_item["fgs_made"] = stat["value"]
            elif stat["name"] == "fieldGoalPct":
                stats_item["fgs_pct"] = stat["value"]
            elif stat["name"] == "freeThrowsAttempted":
                stats_item["fts_attempted"] = stat["value"]
            elif stat["name"] == "freeThrowsMade":
                stats_item["fts_made"] = stat["value"]
            elif stat["name"] == "freeThrowPct":
                stats_item["fts_pct"] = stat["value"]
            elif stat["name"] == "offensiveRebounds":
                stats_item["off_rebounds"] = stat["value"]
            elif stat["name"] == "points":
                stats_item["points"] = stat["value"]
            elif stat["name"] == "turnovers":
                stats_item["turnovers"] = stat["value"]
            elif stat["name"] == "threePointFieldGoalsAttempted":
                stats_item["three_fgs_attempted"] = stat["value"]
            elif stat["name"] == "threePointFieldGoalsMade":
                stats_item["three_fgs_made"] = stat["value"]
            elif stat["name"] == "threePointFieldGoalPct":
                stats_item["three_fgs_pct"] = stat["value"]
            elif stat["name"] == "secondChancePoints":
                stats_item["second_chance_points"] = stat["value"]
            elif stat["name"] == "fastBreakPoints":
                stats_item["fast_break_points"] = stat["value"]
            elif stat["name"] == "offensiveReboundPct":
                stats_item["off_rebounds_pct"] = stat["value"]
            elif stat["name"] == "twoPointFieldGoalsMade":
                stats_item["two_fgs_made"] = stat["value"]
            elif stat["name"] == "twoPointFieldGoalsAttempted":
                stats_item["two_fgs_attempted"] = stat["value"]
            elif stat["name"] == "twoPointFieldGoalPct":
                stats_item["two_fgs_pct"] = stat["value"]
            elif stat["name"] == "shootingEfficiency":
                stats_item["shooting_efficiency"] = stat["value"]
            elif stat["name"] == "scoringEfficiency":
                stats_item["scoring_efficiency"] = stat["value"]

        yield stats_item
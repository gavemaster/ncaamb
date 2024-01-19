import scrapy
import json
from collegebball.items import SpreadMovementItem, TotalMovementItem, MoneyLineMovementItem
import collegebball.utils as utils

class EventOddsLinesSpider(scrapy.Spider):
    name = "event_odds_lines"
    start_urls = []

    def start_requests(self):
        for url in self.start_urls:
            if url["spread_movement_ref"] is not None:
                yield scrapy.Request(url=url["spread_movement_ref"], callback=self.parse_spread_movement, meta={"event_id": url["event_id"],
                                                                                                                "provider_id": url["provider_id"],
                                                                                                                "home_team_id": url["home_team_id"],
                                                                                                                "away_team_id": url["away_team_id"],
                                                                                                                "spread_movement_ref": url["spread_movement_ref"],
                                                                                                                "total_movement_ref": url["total_movement_ref"],
                                                                                                                "moneyline_movement_ref": url["moneyline_movement_ref"]})
            elif url["total_movement_ref"] is not None:
                yield scrapy.Request(url=url["spread_movement_ref"], callback=self.parse_spread_movement, meta={"event_id": url["event_id"],
                                                                                                                "provider_id": url["provider_id"],
                                                                                                                "home_team_id": url["home_team_id"],
                                                                                                                "away_team_id": url["away_team_id"],
                                                                                                                "spread_movement_ref": url["spread_movement_ref"],
                                                                                                                "total_movement_ref": url["total_movement_ref"],
                                                                                                                "moneyline_movement_ref": url["moneyline_movement_ref"]})
            elif url["moneyline_movement_ref"] is not None:
                yield scrapy.Request(url=url["spread_movement_ref"], callback=self.parse_spread_movement, meta={"event_id": url["event_id"],
                                                                                                                "provider_id": url["provider_id"],
                                                                                                                "home_team_id": url["home_team_id"],
                                                                                                                "away_team_id": url["away_team_id"],
                                                                                                                "spread_movement_ref": url["spread_movement_ref"],
                                                                                                                "total_movement_ref": url["total_movement_ref"],
                                                                                                                "moneyline_movement_ref": url["moneyline_movement_ref"]})
            else:
                return

    def parse_spread_movement(self, response):
        items = json.loads(response.body)["items"]
        for item in items:
            spread_movement = SpreadMovementItem()

            spread_movement["event_id"] = response.meta["event_id"]
            spread_movement["home_team_id"] = response.meta["home_team_id"]
            spread_movement["away_team_id"] = response.meta["away_team_id"]
            spread_movement["provider_id"] = response.meta["provider_id"]

            if "homeOdds" in item:
                spread_movement["home_odds"] = item["homeOdds"]
            else:
                spread_movement["home_odds"] = None
            
            if "awayOdds" in item:
                spread_movement["away_odds"] = item["awayOdds"]
            else:
                spread_movement["away_odds"] = None
            
            if "line" in item:
                spread_movement["line"] = item["line"]
            else:
                spread_movement["line"] = None

            if "timestamp" in item:
                spread_movement["timestamp"] = utils.convert_zulu_date_to_est(item["timestamp"])
            else:
                spread_movement["timestamp"] = None
            
            yield spread_movement

            if response.meta["total_movement_ref"] is not None:
                yield scrapy.Request(url=response.meta["total_movement_ref"], callback=self.parse_total_movement, meta={"event_id": response.meta["event_id"],
                                                                                                                        "provider_id": response.meta["provider_id"],
                                                                                                                        "home_team_id": response.meta["home_team_id"],
                                                                                                                        "away_team_id": response.meta["away_team_id"],
                                                                                                                        "spread_movement_ref": response.meta["spread_movement_ref"],
                                                                                                                        "total_movement_ref": response.meta["total_movement_ref"],
                                                                                                                        "moneyline_movement_ref": response.meta["moneyline_movement_ref"]})
            elif response.meta["moneyline_movement_ref"] is not None:
                yield scrapy.Request(url=response.meta["moneyline_movement_ref"], callback=self.parse_moneyline_movement, meta={"event_id": response.meta["event_id"],
                                                                                                                        "provider_id": response.meta["provider_id"],
                                                                                                                        "home_team_id": response.meta["home_team_id"],
                                                                                                                        "away_team_id": response.meta["away_team_id"],
                                                                                                                        "spread_movement_ref": response.meta["spread_movement_ref"],
                                                                                                                        "total_movement_ref": response.meta["total_movement_ref"],
                                                                                                                        "moneyline_movement_ref": response.meta["moneyline_movement_ref"]})
            else:
                return


    def parse_total_movement(self, response):
        items = json.loads(response.body)["items"]

        for item in items:
            total_movement = TotalMovementItem()

            total_movement["event_id"] = response.meta["event_id"]
            total_movement["home_team_id"] = response.meta["home_team_id"]
            total_movement["away_team_id"] = response.meta["away_team_id"]
            total_movement["provider_id"] = response.meta["provider_id"]

            if "overOdds" in item:
                total_movement["over_odds"] = item["overOdds"]
            else:
                total_movement["over_odds"] = None

            if "underOdds" in item:
                total_movement["under_odds"] = item["underOdds"]
            else:
                total_movement["under_odds"] = None

            if "line" in item:    
                total_movement["line"] = item["line"]
            else:
                total_movement["line"] = None
            
            if "timestamp" in item:
                total_movement["timestamp"] = utils.convert_zulu_date_to_est(item["lineDate"])
            else:
                total_movement["timestamp"] = None

            yield total_movement

            if response.meta["moneyline_movement_ref"] is not None:
                yield scrapy.Request(url=response.meta["moneyline_movement_ref"], callback=self.parse_moneyline_movement, meta={"event_id": response.meta["event_id"],
                                                                                                                        "provider_id": response.meta["provider_id"],
                                                                                                                        "home_team_id": response.meta["home_team_id"],
                                                                                                                        "away_team_id": response.meta["away_team_id"],
                                                                                                                        "spread_movement_ref": response.meta["spread_movement_ref"],
                                                                                                                        "total_movement_ref": response.meta["total_movement_ref"],
                                                                                                                        "moneyline_movement_ref": response.meta["moneyline_movement_ref"]})
            else:
                return
            
    def parse_moneyline_movement(self, response):
        items = json.loads(response.body)["items"]

        for item in items:
            money_line_movement = MoneyLineMovementItem()

            money_line_movement["event_id"] = response.meta["event_id"]
            money_line_movement["home_team_id"] = response.meta["home_team_id"]
            money_line_movement["away_team_id"] = response.meta["away_team_id"]
            money_line_movement["provider_id"] = response.meta["provider_id"]

            if "homeOdds" in item:
                money_line_movement["home_odds"] = item["homeOdds"]
            else:
                money_line_movement["home_odds"] = None
            
            if "awayOdds" in item:
                money_line_movement["away_odds"] = item["awayOdds"]
            else:
                money_line_movement["away_odds"] = None
            
            if "timestamp" in item:
                money_line_movement["timestamp"] = utils.convert_zulu_date_to_est(item["lineDate"])
            else:
                money_line_movement["timestamp"] = None
            yield money_line_movement
            
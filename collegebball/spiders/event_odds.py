import scrapy
import json
from collegebball.utils import convert_zulu_date_to_est

from collegebball.items import EventOddsItem, MoneyLineHistoryItem, MoneyLineMovementItem, SpreadHistoryItem, SpreadMovementItem, TotalHistoryItem, TotalMovementItem

class EventOddsSpider(scrapy.Spider):
    name = "event_odds"
    start_urls = []

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url["odds_ref"], callback=self.parse, meta={"event_id": url["event_id"], "home_team": url["home_team"], 
                                                                                 "away_team": url["away_team"]})

    def parse(self, response):
        odds_data = json.loads(response.body)["items"]

        for odds in odds_data:
            event_odds = EventOddsItem()
            
            if "awayTeamOdds" in odds:
                away_team_odds = odds["awayTeamOdds"]
                if "moneyLine" in away_team_odds:
                    event_odds["away_team_moneyline"] = away_team_odds["moneyLine"]
                else:
                    event_odds["away_team_moneyline"] = None
                if "spreadOdds" in away_team_odds:
                    event_odds["away_team_spread_odds"] = away_team_odds["spreadOdds"]
                else:
                    event_odds["away_team_spread_odds"] = None
            else:
                event_odds["away_team_moneyline"] = None
                event_odds["away_team_spread_odds"] = None
            
            if "homeTeamOdds" in odds:
                home_team_odds = odds["homeTeamOdds"]
                if "moneyLine" in home_team_odds:
                    event_odds["home_team_moneyline"] = home_team_odds["moneyLine"]
                else:
                    event_odds["home_team_moneyline"] = None
                if "spreadOdds" in home_team_odds:
                    event_odds["home_team_spread_odds"] = home_team_odds["spreadOdds"]
                else:
                    event_odds["home_team_spread_odds"] = None
            else:
                event_odds["home_team_moneyline"] = None
                event_odds["home_team_spread_odds"] = None
            
            if "moneyLineHistory" in odds:
                money_line_history = odds["moneyLineHistory"]
                event_odds["moneyline_history_ref"] = money_line_history["$ref"]
            else:
                event_odds["moneyline_history_ref"] = None
            
            if "spreadHistory" in odds:
                spread_history = odds["spreadHistory"]
                event_odds["spread_history_ref"] = spread_history["$ref"]
            else:
                event_odds["spread_history_ref"] = None

            if "totalHistory" in odds:
                total_history = odds["totalHistory"]
                event_odds["total_history_ref"] = total_history["$ref"]
            else:
                event_odds["total_history_ref"] = None
            
            if "spread" in odds:
                event_odds["home_team_spread"] = odds["spread"]
                event_odds["away_team_spread"] = -odds["spread"]
            else:
                event_odds["home_team_spread"] = None
                event_odds["away_team_spread"] = None
            
            if "overUnder" in odds:
                event_odds["over_under"] = odds["overUnder"]
            else:
                event_odds["over_under"] = None
            
            
            if "overOdds" in odds:
                event_odds["over_odds"] = odds["overOdds"]
            else:
                event_odds["over_odds"] = None
            
            if "underOdds" in odds:
                event_odds["under_odds"] = odds["underOdds"]
            else:
                event_odds["under_odds"] = None


            if "provider" in odds:
                event_odds["provider_id"] = odds["provider"]["id"]
            else:
                event_odds["provider_id"] = None

            event_odds["event_id"] = response.meta["event_id"]
            event_odds["home_team_id"] = response.meta["home_team"]
            event_odds["away_team_id"] = response.meta["away_team"]
            

            #spread_history_ref = event_odds["spread_history_ref"]
            #total_history_ref = event_odds["total_history_ref"]
            #money_line_history_ref = event_odds["moneyline_history_ref"]
            #provider_id = event_odds["provider_id"]
            #event_id = event_odds["event_id"]
            #home_team_id = event_odds["home_team_id"]
            #away_team_id = event_odds["away_team_id"]

            yield event_odds    

            #if spread_history_ref is not None:
            #    yield scrapy.Request(url=spread_history_ref, callback=self.parse_spread_history, meta={"event_id": event_id, "home_team_id": home_team_id, "away_team_id": away_team_id, "provider_id": provider_id})
            #
            #if total_history_ref is not None:
            #    yield scrapy.Request(url=total_history_ref, callback=self.parse_total_history, meta={"event_id": event_id, "home_team_id": home_team_id, "away_team_id": away_team_id, "provider_id": provider_id})
            #
            #if money_line_history_ref is not None:
            #    yield scrapy.Request(url=money_line_history_ref, callback=self.parse_moneyline_history, meta={"event_id": event_id, "home_team_id": home_team_id, "away_team_id": away_team_id, "provider_id": provider_id})
        


    def parse_spread_movement(self, response):
        items = json.loads(response.body)["items"]
        for item in items:
            spread_movement = SpreadMovementItem()

            spread_movement["event_id"] = response.meta["event_id"]
            spread_movement["home_team_id"] = response.meta["home_team_id"]
            spread_movement["away_team_id"] = response.meta["away_team_id"]
            spread_movement["provider_id"] = response.meta["provider_id"]

            spread_movement["home_odds"] = item["homeOdds"]
            spread_movement["away_odds"] = item["awayOdds"]
            spread_movement["line"] = item["line"]
            spread_movement["timestamp"] = convert_zulu_date_to_est(item["lineDate"])
            yield spread_movement
    


    def parse_total_movement(self, response):
        items = json.loads(response.body)["items"]

        for item in items:
            total_movement = TotalMovementItem()

            total_movement["event_id"] = response.meta["event_id"]
            total_movement["home_team_id"] = response.meta["home_team_id"]
            total_movement["away_team_id"] = response.meta["away_team_id"]
            total_movement["provider_id"] = response.meta["provider_id"]

            total_movement["over_odds"] = item["overOdds"]
            total_movement["under_odds"] = item["underOdds"]
            total_movement["line"] = item["line"]
            total_movement["timestamp"] = convert_zulu_date_to_est(item["lineDate"])
            yield total_movement
        


    
    def parse_money_line_movement(self, response):
        items = json.loads(response.body)["items"]

        for item in items:
            money_line_movement = MoneyLineMovementItem()

            money_line_movement["event_id"] = response.meta["event_id"]
            money_line_movement["home_team_id"] = response.meta["home_team_id"]
            money_line_movement["away_team_id"] = response.meta["away_team_id"]
            money_line_movement["provider_id"] = response.meta["provider_id"]

            money_line_movement["home_odds"] = item["homeOdds"]
            money_line_movement["away_odds"] = item["awayOdds"]
            money_line_movement["timestamp"] = convert_zulu_date_to_est(item["lineDate"])
            yield money_line_movement
            
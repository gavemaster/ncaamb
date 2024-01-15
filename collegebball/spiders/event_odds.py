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
                if "spreadOdds" in away_team_odds:
                    event_odds["away_team_spread_odds"] = away_team_odds["spreadOdds"]
            
            if "homeTeamOdds" in odds:
                home_team_odds = odds["homeTeamOdds"]
                if "moneyLine" in home_team_odds:
                    event_odds["home_team_moneyline"] = home_team_odds["moneyLine"]
                if "spreadOdds" in home_team_odds:
                    event_odds["home_team_spread_odds"] = home_team_odds["spreadOdds"]
            
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
            
            if "overUnder" in odds:
                event_odds["over_under"] = odds["overUnder"]
            
            if "overOdds" in odds:
                event_odds["over_odds"] = odds["overOdds"]
            
            if "underOdds" in odds:
                event_odds["under_odds"] = odds["underOdds"]

            if "provider" in odds:
                event_odds["provider_id"] = odds["provider"]["id"]
            else:
                event_odds["provider_id"] = None

            event_odds["event_id"] = response.meta["event_id"]
            event_odds["home_team_id"] = response.meta["home_team"]
            event_odds["away_team_id"] = response.meta["away_team"]
            

            spread_history_ref = event_odds["spread_history_ref"]
            total_history_ref = event_odds["total_history_ref"]
            money_line_history_ref = event_odds["moneyline_history_ref"]
            provider_id = event_odds["provider_id"]
            event_id = event_odds["event_id"]
            home_team_id = event_odds["home_team_id"]
            away_team_id = event_odds["away_team_id"]

            yield event_odds    

            if spread_history_ref is not None:
                yield scrapy.Request(url=spread_history_ref, callback=self.parse_spread_history, meta={"event_id": event_id, "home_team_id": home_team_id, "away_team_id": away_team_id, "provider_id": provider_id})
            
            if total_history_ref is not None:
                yield scrapy.Request(url=total_history_ref, callback=self.parse_total_history, meta={"event_id": event_id, "home_team_id": home_team_id, "away_team_id": away_team_id, "provider_id": provider_id})
            
            if money_line_history_ref is not None:
                yield scrapy.Request(url=money_line_history_ref, callback=self.parse_moneyline_history, meta={"event_id": event_id, "home_team_id": home_team_id, "away_team_id": away_team_id, "provider_id": provider_id})
        

    def parse_spread_history(self, response):
        data = json.loads(response.body)
        highlights = data["highlights"]
        away_team = highlights["awayTeam"]
        home_team = highlights["homeTeam"]

        spread_history = SpreadHistoryItem()

        spread_history["event_id"] = response.meta["event_id"]
        spread_history["home_team_id"] = response.meta["home_team_id"]
        spread_history["away_team_id"] = response.meta["away_team_id"]
        spread_history["provider_id"] = response.meta["provider_id"]

        spread_history["away_team_spread_odds_low"] = away_team["low"]
        spread_history["away_team_spread_odds_high"] = away_team["high"]
        spread_history["away_team_spread_odds_open"] = away_team["open"]
        spread_history["away_team_spread_odds_current"] = away_team["current"]
        spread_history["away_team_spread_odds_prev"] = away_team["previous"]
        spread_history["home_team_spread_odds_low"] = home_team["low"]
        spread_history["home_team_spread_odds_high"] = home_team["high"]
        spread_history["home_team_spread_odds_open"] = home_team["open"] 
        spread_history["home_team_spread_odds_current"] = home_team["current"]
        spread_history["home_team_spread_odds_prev"] = home_team["previous"] 

        if "movement" in data:
            spread_history["movement_ref"] = data["movement"]["$ref"]
            yield spread_history
            yield scrapy.Request(url=data["movement"]["$ref"], callback=self.parse_spread_movement, meta={"event_id": response.meta["event_id"], "home_team_id": response.meta["home_team_id"], "away_team_id": response.meta["away_team_id"], "provider_id": response.meta["provider_id"]})
        else:
            spread_history["movement_ref"] = None
            yield spread_history


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
    
    def parse_total_history(self, response):
        data = json.loads(response.body)
        highlights = data["highlights"]

        total_history = TotalHistoryItem()

        total_history["event_id"] = response.meta["event_id"]
        total_history["home_team_id"] = response.meta["home_team_id"]
        total_history["away_team_id"] = response.meta["away_team_id"]
        total_history["provider_id"] = response.meta["provider_id"]

        total_history["total_low"] = highlights["low"]
        total_history["total_high"] = highlights["high"]
        total_history["total_open"] = highlights["open"]
        total_history["total_current"] = highlights["current"]
        total_history["total_prev"] = highlights["previous"]
        
        if "movement" in data:
            total_history["movement_ref"] = data["movement"]["$ref"]
            yield total_history
            yield scrapy.Request(url=data["movement"]["$ref"], callback=self.parse_total_movement, meta={"event_id": response.meta["event_id"], "home_team_id": response.meta["home_team_id"], "away_team_id": response.meta["away_team_id"], "provider_id": response.meta["provider_id"]})
        else:
            total_history["movement_ref"] = None
            yield total_history

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
        
    def parse_moneyline_history(self, response):
        data = json.loads(response.body)
        highlights = data["highlights"]
        home_team = highlights["homeTeam"]
        away_team = highlights["awayTeam"]
        money_line_history = MoneyLineHistoryItem()

        money_line_history["event_id"] = response.meta["event_id"]
        money_line_history["home_team_id"] = response.meta["home_team_id"]
        money_line_history["away_team_id"] = response.meta["away_team_id"]
        money_line_history["provider_id"] = response.meta["provider_id"]

        money_line_history["home_team_low"] = home_team["low"]
        money_line_history["home_team_high"] = home_team["high"]
        money_line_history["home_team_open"] = home_team["open"]
        money_line_history["home_team_current"] = home_team["current"]
        money_line_history["home_team_prev"] = home_team["previous"]
        money_line_history["away_team_low"] = away_team["low"]
        money_line_history["away_team_high"] = away_team["high"]
        money_line_history["away_team_open"] = away_team["open"]
        money_line_history["away_team_current"] = away_team["current"]
        money_line_history["away_team_prev"] = away_team["previous"]

        if "movement" in data:
            money_line_history["movement_ref"] = data["movement"]["$ref"]
            yield money_line_history
            yield scrapy.Request(url=data["movement"]["$ref"], callback=self.parse_money_line_movement, meta={"event_id": response.meta["event_id"], "home_team_id": response.meta["home_team_id"], "away_team_id": response.meta["away_team_id"], "provider_id": response.meta["provider_id"]})
        else:
            money_line_history["movement_ref"] = None
            yield money_line_history

    
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
            
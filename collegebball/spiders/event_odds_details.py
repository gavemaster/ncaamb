import scrapy
import json

from collegebball.items import SpreadHistoryItem, MoneyLineHistoryItem, TotalHistoryItem


class EventOddsDetailsSpider(scrapy.Spider):
    name = "event_odds_details"
    start_urls = []

    def start_requests(self):
        for url in self.start_urls:
            if url["spread_details_ref"] is not None:
                yield scrapy.Request(url=url["spread_details_ref"], callback=self.parse_spread_details, meta={"event_id": url["event_id"], "home_team_id": url["home_team_id"], "away_team_id": url["away_team_id"], "provider_id": url["provider_id"],
                                                                                                                "spread_details_ref": url["spread_details_ref"], "moneyline_details_ref": url["moneyline_details_ref"], "total_details_ref": url["total_details_ref"]})
            elif url["moneyline_details_ref"] is not None:
                yield scrapy.Request(url=url["moneyline_details_ref"], callback=self.parse_moneyline_details, meta={"event_id": url["event_id"], "home_team_id": url["home_team_id"], "away_team_id": url["away_team_id"], "provider_id": url["provider_id"],
                                                                                                                "spread_details_ref": url["spread_details_ref"], "moneyline_details_ref": url["moneyline_details_ref"], "total_details_ref": url["total_details_ref"]})
            elif url["total_details_ref"] is not None:
                yield scrapy.Request(url=url["total_details_ref"], callback=self.parse_total_details, meta={"event_id": url["event_id"], "home_team_id": url["home_team_id"], "away_team_id": url["away_team_id"], "provider_id": url["provider_id"],
                                                                                                                "spread_details_ref": url["spread_details_ref"], "moneyline_details_ref": url["moneyline_details_ref"], "total_details_ref": url["total_details_ref"]})
            else:
                continue
            

    
    def parse_spread_details(self, response):
        data = json.loads(response.body)
        highlights = data["highlights"]
        away_team = highlights["awayTeam"]
        home_team = highlights["homeTeam"]

        spread_history = SpreadHistoryItem()

        spread_history["event_id"] = response.meta["event_id"]
        spread_history["home_team_id"] = response.meta["home_team_id"]
        spread_history["away_team_id"] = response.meta["away_team_id"]
        spread_history["provider_id"] = response.meta["provider_id"]


        if "low" in away_team:
            spread_history["away_team_spread_odds_low"] = away_team["low"]
        else:
            spread_history["away_team_spread_odds_low"] = None

        
        if "high" in away_team:
            spread_history["away_team_spread_odds_high"] = away_team["high"]
        else:
            spread_history["away_team_spread_odds_high"] = None
        
        if "open" in away_team:
            spread_history["away_team_spread_odds_open"] = away_team["open"]
        else:
            spread_history["away_team_spread_odds_open"] = None

        if "current" in away_team:
            spread_history["away_team_spread_odds_current"] = away_team["current"]
        else:
            spread_history["away_team_spread_odds_current"] = None

        if "previous" in away_team:
            spread_history["away_team_spread_odds_prev"] = away_team["previous"]
        else:
            spread_history["away_team_spread_odds_prev"] = None
        
        if "low" in home_team:
            spread_history["home_team_spread_odds_low"] = home_team["low"]
        else:
            spread_history["home_team_spread_odds_low"] = None

        if "high" in home_team:
            spread_history["home_team_spread_odds_high"] = home_team["high"]
        else:
            spread_history["home_team_spread_odds_high"] = None
        
        if "open" in home_team:
            spread_history["home_team_spread_odds_open"] = home_team["open"]
        else:
            spread_history["home_team_spread_odds_open"] = None

        if "current" in home_team:
            spread_history["home_team_spread_odds_current"] = home_team["current"]
        else:
            spread_history["home_team_spread_odds_current"] = None


        if "previous" in home_team: 
            spread_history["home_team_spread_odds_prev"] = home_team["previous"]
        else:
            spread_history["home_team_spread_odds_prev"] = None 

        if "movement" in data:
            spread_history["movement_ref"] = data["movement"]["$ref"]
        else:
            spread_history["movement_ref"] = None

        yield spread_history
    
        if response.meta["moneyline_details_ref"] is not None:
            yield scrapy.Request(url=response.meta["moneyline_details_ref"], callback=self.parse_moneyline_details, meta={"event_id": response.meta["event_id"], "home_team_id": response.meta["home_team_id"], "away_team_id": response.meta["away_team_id"], "provider_id": response.meta["provider_id"],
                                                                                                                "spread_details_ref": response.meta["spread_details_ref"], "moneyline_details_ref": response.meta["moneyline_details_ref"], "total_details_ref": response.meta["total_details_ref"]})
        elif response.meta["total_details_ref"] is not None:
            yield scrapy.Request(url=response.meta["total_details_ref"], callback=self.parse_total_details, meta={"event_id": response.meta["event_id"], "home_team_id": response.meta["home_team_id"], "away_team_id": response.meta["away_team_id"], "provider_id": response.meta["provider_id"],
                                                                                                                "spread_details_ref": response.meta["spread_details_ref"], "moneyline_details_ref": response.meta["moneyline_details_ref"], "total_details_ref": response.meta["total_details_ref"]})
        else:
            return
        
    def parse_total_details(self, response):
        data = json.loads(response.body)
        highlights = data["highlights"]

        total_history = TotalHistoryItem()

        total_history["event_id"] = response.meta["event_id"]
        total_history["home_team_id"] = response.meta["home_team_id"]
        total_history["away_team_id"] = response.meta["away_team_id"]
        total_history["provider_id"] = response.meta["provider_id"]

        if "low" in highlights:
            total_history["total_low"] = highlights["low"]
        else:
            total_history["total_low"] = None
        
        if "high" in highlights:
            total_history["total_high"] = highlights["high"]
        else:
            total_history["total_high"] = None
        
        if "open" in highlights:
            total_history["total_open"] = highlights["open"]
        else:
            total_history["total_open"] = None
        
        if "current" in highlights:
            total_history["total_current"] = highlights["current"]
        else:
            total_history["total_current"] = None
        
        if "previous" in highlights:
            total_history["total_prev"] = highlights["previous"]
        else:
            total_history["total_prev"] = None
        
        if "movement" in data:
            total_history["movement_ref"] = data["movement"]["$ref"]
        else:
            total_history["movement_ref"] = None
        

        yield total_history



    def parse_moneyline_details(self, response):
        data = json.loads(response.body)
        highlights = data["highlights"]
        home_team = highlights["homeTeam"]
        away_team = highlights["awayTeam"]
        money_line_history = MoneyLineHistoryItem()

        money_line_history["event_id"] = response.meta["event_id"]
        money_line_history["home_team_id"] = response.meta["home_team_id"]
        money_line_history["away_team_id"] = response.meta["away_team_id"]
        money_line_history["provider_id"] = response.meta["provider_id"]

        if "low" in home_team:
            money_line_history["home_team_low"] = home_team["low"]
        else:
            money_line_history["home_team_low"] = None

        if "high" in home_team:
            money_line_history["home_team_high"] = home_team["high"]
        else:
            money_line_history["home_team_high"] = None

        if "open" in home_team:
            money_line_history["home_team_open"] = home_team["open"]
        else:
            money_line_history["home_team_open"] = None

        if "current" in home_team:
            money_line_history["home_team_current"] = home_team["current"]
        else:
            money_line_history["home_team_current"] = None
        if "previous" in home_team:
            money_line_history["home_team_prev"] = home_team["previous"]
        else:
            money_line_history["home_team_prev"] = None

        if "low" in away_team:
            money_line_history["away_team_low"] = away_team["low"]
        else:
            money_line_history["away_team_low"] = None

        if "high" in away_team:
            money_line_history["away_team_high"] = away_team["high"]
        else:
            money_line_history["away_team_high"] = None

        if "open" in away_team:
            money_line_history["away_team_open"] = away_team["open"]
        else:
            money_line_history["away_team_open"] = None
        
        if "current" in away_team:
            money_line_history["away_team_current"] = away_team["current"]
        else:
            money_line_history["away_team_current"] = None

        if "previous" in away_team:
            money_line_history["away_team_prev"] = away_team["previous"]
        else:
            money_line_history["away_team_prev"] = None


        if "movement" in data:
            money_line_history["movement_ref"] = data["movement"]["$ref"]
            
        else:
            money_line_history["movement_ref"] = None

        yield money_line_history

        if response.meta["total_details_ref"] is not None:
            yield scrapy.Request(url=response.meta["total_details_ref"], callback=self.parse_total_details, meta={"event_id": response.meta["event_id"], "home_team_id": response.meta["home_team_id"], "away_team_id": response.meta["away_team_id"], "provider_id": response.meta["provider_id"],
                                                                                                                "spread_details_ref": response.meta["spread_details_ref"], "moneyline_details_ref": response.meta["moneyline_details_ref"], "total_details_ref": response.meta["total_details_ref"]})
        else:
            return
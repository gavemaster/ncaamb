# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from collegebball.items import (

    ConferenceItem,
    EventOddsItem,
    MoneyLineHistoryItem,
    MoneyLineMovementItem,
    SpreadHistoryItem,
    SpreadMovementItem,
    TeamItem,
    TeamOutcomesItem,
    EventItem,
    EventDetailsItem,
    TotalHistoryItem,
    TotalMovementItem,
    VenueItem,
    AthleteItem,
    RosterItem,
    PlayerStatsItem,
    BookieItem
)
import collegebball.database as db
from dotenv import load_dotenv
import datetime
import os
from scrapy.exceptions import DropItem
import logging
from MySQLdb._exceptions import IntegrityError


load_dotenv()
# Configure logging
logging.basicConfig(level=logging.INFO)

# Setup success logger
success_logger = logging.getLogger("items")
success_logger.setLevel(logging.INFO)
success_log_handler = logging.FileHandler(
    "items.log", mode="w"
)  # Overwrite the log file
success_log_handler.setLevel(logging.INFO)
success_logger.addHandler(success_log_handler)

# Setup error logger
error_logger = logging.getLogger("errors")
error_logger.setLevel(logging.ERROR)
error_log_handler = logging.FileHandler(
    "errors.log", mode="w"
)  # Overwrite the log file
error_log_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_log_handler)


# Setup foreign key logger
foreign_key_logger = logging.getLogger("foreign_keys")
foreign_key_logger.setLevel(logging.INFO)
foreign_key_log_handler = logging.FileHandler(
    "foreign_keys.log", mode="w"
)  # Overwrite the log file for foreign key errors
foreign_key_log_handler.setLevel(logging.ERROR)
foreign_key_logger.addHandler(foreign_key_log_handler)




class MySQLStorePipeline(object):
    def __init__(self):
        self.dbpool = db.get_db_pool()
        self.success_logger = success_logger
        self.error_logger = error_logger
        self.foreign_key_logger = foreign_key_logger
        
    def process_item(self, item, spider):
        success_logger.info(f"Processing items in pipeline")
        if isinstance(item, TeamItem):
            success_logger.info(f"Processing team item... inserting college first")
            team_query = self.dbpool.runInteraction(self._college_insert, item)
            team_query.addErrback(self._handle_error, item)
            team_query.addCallback(self._college_success, item)

        elif isinstance(item, ConferenceItem):
            success_logger.info(f"Processing conference item")
            conf_query = self.dbpool.runInteraction(self._conference_insert, item)
            conf_query.addErrback(self._handle_error, item)
            conf_query.addCallback(self._conference_success, item)

        elif isinstance(item, TeamOutcomesItem):
            success_logger.info(f"Processing team outcomes item")
            record_query = self.dbpool.runInteraction(self._record_insert, item)
            record_query.addErrback(self._handle_error, item)
            record_query.addCallback(self._success, item)


        elif isinstance(item, EventItem):
            success_logger.info(f"Processing event item")

            if item["venue_id"] is None:
                event_query = self.dbpool.runInteraction(self._event_insert, item)
                event_query.addErrback(self._handle_error, item)
                event_query.addCallback(self._event_success, item)
            else:
                event_query = self.dbpool.runInteraction(self._venue_insert, item)
                event_query.addErrback(self._handle_error, item)
                event_query.addCallback(self._venue_success, item)

        elif isinstance(item, EventDetailsItem):
            success_logger.info(f"Processing event details item")
            event_details_query = self.dbpool.runInteraction(self._event_details_insert, item)
            event_details_query.addErrback(self._handle_error, item)
            event_details_query.addCallback(self._success, item)

        elif isinstance(item, VenueItem):
            success_logger.info(f"Processing venue item")
            venue_query = self.dbpool.runInteraction(self._venue_insert, item)
            venue_query.addErrback(self._handle_error, item)
            venue_query.addCallback(self._success, item)

        elif isinstance(item, AthleteItem):
            success_logger.info(f"Processing athlete item")
            athlete_query = self.dbpool.runInteraction(self._athlete_insert, item)
            athlete_query.addErrback(self._handle_error, item)
            athlete_query.addCallback(self._success, item)

        elif isinstance(item, RosterItem):
            success_logger.info(f"Processing roster item")
            roster_query = self.dbpool.runInteraction(self._roster_insert, item)
            roster_query.addErrback(self._handle_error, item)
            roster_query.addCallback(self._success, item)

        elif isinstance(item, PlayerStatsItem):
            success_logger.info(f"Processing player stats item")
            player_stats_query = self.dbpool.runInteraction(self._player_stats_insert, item)
            player_stats_query.addErrback(self._handle_error, item)
            player_stats_query.addCallback(self._success, item)
        
        elif isinstance(item, BookieItem):
            success_logger.info(f"Processing bookie item")
            bookie_query = self.dbpool.runInteraction(self._bookie_insert, item)
            bookie_query.addErrback(self._handle_error, item)
            bookie_query.addCallback(self._success, item)

        elif isinstance(item, EventOddsItem):
            success_logger.info(f"Processing event odds item")
            event_odds_query = self.dbpool.runInteraction(self._event_odds_insert, item)
            event_odds_query.addErrback(self._handle_error, item)
            event_odds_query.addCallback(self._success, item)

        elif isinstance(item, SpreadHistoryItem):
            success_logger.info(f"Processing spread history item")
            spread_history_query = self.dbpool.runInteraction(self._spread_history_insert, item)
            spread_history_query.addErrback(self._handle_error, item)
            spread_history_query.addCallback(self._success, item)

        elif isinstance(item, TotalHistoryItem):
            success_logger.info(f"Processing total history item")
            total_history_query = self.dbpool.runInteraction(self._total_history_insert, item)
            total_history_query.addErrback(self._handle_error, item)
            total_history_query.addCallback(self._success, item)
        
        elif isinstance(item, MoneyLineHistoryItem):
            success_logger.info(f"Processing money line history item")
            money_line_history_query = self.dbpool.runInteraction(self._moneyline_history_insert, item)
            money_line_history_query.addErrback(self._handle_error, item)
            money_line_history_query.addCallback(self._success, item)

        elif isinstance(item, SpreadMovementItem):
            success_logger.info(f"Processing spread movement item")
            spread_movement_query = self.dbpool.runInteraction(self._spread_movement_insert, item)
            spread_movement_query.addErrback(self._handle_error, item)
            spread_movement_query.addCallback(self._success, item)
        
        elif isinstance(item, TotalMovementItem):
            success_logger.info(f"Processing total movement item")
            total_movement_query = self.dbpool.runInteraction(self._total_movement_insert, item)
            total_movement_query.addErrback(self._handle_error, item)
            total_movement_query.addCallback(self._success, item)

        elif isinstance(item, MoneyLineMovementItem):
            success_logger.info(f"Processing moneyline movement item")
            moneyline_movement_query = self.dbpool.runInteraction(self._moneyline_movement_insert, item)
            moneyline_movement_query.addErrback(self._handle_error, item)
            moneyline_movement_query.addCallback(self._success, item)

    def _college_insert(self, tx, item):
        # Insert data to the database depending on conditions
        # Here you would put your MySQL INSERT statement
        sql = db.COLLEGE_INSERT_QUERY
        args = (item["college_espn_id"], item["team_name"], item["team_location"])
        tx.execute(sql, args)

    def _team_insert(self, tx, item):
        # Insert data to the database depending on conditions
        # Here you would put your MySQL INSERT statement
        sql = db.TEAM_UPSERT_QUERY
        args = (
            item["team_name"],
            item["college_espn_id"],
            item["team_location"],
            item["team_conference"],
            item["team_abbr"],
            item["season"],
            item["team_events_ref"],
            item["conference_ref"],
            item["record_ref"], 
            item["ats_ref"], 
            item["ranks_ref"] 
        )
        tx.execute(sql, args)

    def _record_insert(self, tx, item):
        # Insert data to the database depending on conditions
        # Here you would put your MySQL UPSERT statement
        last_updated = datetime.datetime.now()
        sql = db.TEAM_RESULTS_UPSERT_QUERY
        args = (
            item["college_espn_id"],
            item["season"],
            item["ats_overall_wins"],
            item["ats_overall_losses"],
            item["ats_overall_pushes"],
            item["ats_favorite_wins"],
            item["ats_favorite_losses"],
            item["ats_favorite_pushes"],
            item["ats_underdog_wins"],
            item["ats_underdog_losses"],
            item["ats_underdog_pushes"],
            item["ats_away_wins"],
            item["ats_away_losses"],
            item["ats_away_pushes"],
            item["ats_home_wins"],
            item["ats_home_losses"],
            item["ats_home_pushes"],
            item["ats_away_favorite_wins"],
            item["ats_away_favorite_losses"],
            item["ats_away_favorite_pushes"],
            item["ats_away_underdog_wins"],
            item["ats_away_underdog_losses"],
            item["ats_away_underdog_pushes"],
            item["ats_home_favorite_wins"],
            item["ats_home_favorite_losses"],
            item["ats_home_favorite_pushes"],
            item["ats_home_underdog_wins"],
            item["ats_home_underdog_losses"],
            item["ats_home_underdog_pushes"],
            item["team_ovr_wins"],
            item["team_ovr_losses"],
            item["team_ovr_ot_losses"],
            item["team_ovr_ot_wins"],
            item["team_ovr_point_diff"],
            item["team_ovr_avg_points"],
            item["team_ovr_avg_points_allowed"],
            item["team_ovr_division_win_percentage"],
            item["team_ovr_games_played"],
            item["team_home_wins"],
            item["team_home_losses"],
            item["team_home_ot_losses"],
            item["team_home_ot_wins"],
            item["team_home_point_diff"],
            item["team_home_avg_points"],
            item["team_home_avg_points_allowed"],
            item["team_home_division_win_percentage"],
            item["team_home_games_played"],
            item["team_away_wins"],
            item["team_away_losses"],
            item["team_away_ot_losses"],
            item["team_away_ot_wins"],
            item["team_away_point_diff"],
            item["team_away_avg_points"],
            item["team_away_avg_points_allowed"],
            item["team_away_division_win_percentage"],
            item["team_away_games_played"],
            item["team_versus_ap_top_25_wins"],
            item["team_versus_ap_top_25_losses"],
            item["team_versus_ap_top_25_point_diff"],
            item["team_versus_ap_top_25_avg_points"],
            item["team_versus_ap_top_25_avg_points_allowed"],
            item["team_versus_ap_top_25_games_played"],
            item["team_versus_conf_wins"],
            item["team_versus_conf_losses"],
            item["team_versus_conf_ot_losses"],
            item["team_versus_conf_ot_wins"],
            item["team_versus_conf_point_diff"],
            item["team_versus_conf_avg_points"],
            item["team_versus_conf_avg_points_allowed"],
            item["team_versus_conf_games_played"],
            last_updated,
        )
        tx.execute(sql, args)

    def _conference_insert(self, tx, item):
        sql = db.CONFERENCE_INSERT_QUERY
        args = (item["conference_id"], item["name"])
        tx.execute(sql, args)

    def _conference_details_insert(self, tx, item):
        last_updated = datetime.datetime.now()
        sql = db.CONFERENCE_DETAILS_UPSERT_QUERY
        args = (
            item["conference_id"],
            item["name"],
            item["short_name"],
            item["conference_standings_ref"],
            item["conference_teams_ref"],
            item["season"],
            last_updated,
        )
        tx.execute(sql, args)

    def _event_insert(self, tx, item):
        sql = db.EVENT_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["event_id"],
            item["name"],
            item["shortname"],
            item["event_date"],
            item["event_time"],
            item["venue_id"],
            item["home_team_college_id"],
            item["away_team_college_id"],
            item["home_team_score"],
            item["away_team_score"],
            item["season"],
            item["season_type"],
            item["week"],
            last_updated,
        )
        tx.execute(sql, args)

    def _event_details_insert(self, tx, item):
        sql = db.EVENT_DETAILS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["event_id"],
            item["odds_ref"],
            item["neutral_site"],
            item["division_game"],
            item["conference_game"],
            item["home_team_stats_ref"],
            item["away_team_stats_ref"],
            item["home_team_roster_ref"],
            item["away_team_roster_ref"],
            item["event_details_ref"],
            item["event_ref"],
            item["status"],
            item["home_team_rank"],
            item["away_team_rank"],
            item["home_team_win"],
            item["away_team_win"],
            item["home_team_record_ref"],
            item["away_team_record_ref"],
            last_updated,
        )
        tx.execute(sql, args)

    def _venue_insert(self, tx, item):
        sql = db.VENUE_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["venue_id"],
            item["venue_name"],
            item["venue_city"],
            item["venue_state"],
            item["venue_capacity"],
            item["venue_indoor"],
            item["venue_ref"],
            last_updated,
        )
        tx.execute(sql, args)

    def _athlete_insert(self, tx, item):
        sql = db.ATHLETE_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["id"],
            item["first_name"],
            item["last_name"],
            item["name"],
            item["birthplace"],
            item["athlete_ref"],
            last_updated,
            item["headshot"],
        )

        tx.execute(sql, args)

    def _athlete_details_insert(self, tx, item):
        sql = db.ATHLETE_DETAILS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["id"],
            item["season"],
            item["team_id"],
            item["jersey_number"],
            item["position"],
            item["height"],
            item["weight"],
            item["exp"],
            item["class_year"],
            item["status"],
            last_updated,
        )

        tx.execute(sql, args)

    def _athlete_team_insert(self, tx, item, sql):
        args = (
            item["season"],
            item["team_id"],
        )
        tx.execute(sql, args)

    def _roster_insert(self, tx, item):
        last_updated = datetime.datetime.now()
        sql = db.ROSTER_UPSERT_QUERY
        args = (
            item["team_id"],
            item["athlete_id"],
            item["season"],
            item["event_id"],
            item["did_not_play"],
            item["stats_ref"],
            item["starter"],
            item["ejected"],
            last_updated,
            item["athlete_ref"]
        )
        tx.execute(sql, args)

    def _player_stats_insert(self, tx, item):
        last_updated = datetime.datetime.now()
        sql = db.PLAYER_STATS_UPSERT_QUERY
        args = (
            item["athlete_id"],
            item["team_id"],
            item["season"],
            item["event_id"],
            item["blocks"],
            item["def_rebounds"],
            item["steals"],
            item["points_off_turnovers"],
            item["flagrant_fouls"],
            item["fouls"],
            item["ejections"],
            item["technical_fouls"],
            item["tot_rebounds"],
            item["total_minutes"],
            item["fantasy_rating"],
            item["plus_minus"],
            item["assist_turnover_ratio"],
            item["steal_foul_ratio"],
            item["blocks_foul_ratio"],
            item["steal_turnover_ratio"],
            item["games_played"],
            item["games_started"],
            item["double_double"],
            item["triple_double"],
            item["assists"],
            item["fgs_attempted"],
            item["fgs_made"],
            item["fgs_pct"],
            item["fts_attempted"],
            item["fts_made"],
            item["fts_pct"],
            item["off_rebounds"],
            item["points"],
            item["turnovers"],
            item["three_fgs_attempted"],
            item["three_fgs_made"],
            item["three_fgs_pct"],
            item["second_chance_points"],
            item["fast_break_points"],
            item["off_rebounds_pct"],
            item["two_fgs_attempted"],
            item["two_fgs_made"],
            item["two_fgs_pct"],
            item["shooting_efficiency"],
            item["scoring_efficiency"],
            last_updated,
            
        )
        tx.execute(sql, args)

    def _bookie_insert(self, tx, item):
        sql = db.BOOKIE_UPSERT_QUERY
        args = (
                item["id"],
                item["name"],
                item["bookie_ref"],
                item["priority"]
        )
        tx.execute(sql, args)

    def _event_odds_insert(self, tx, item):
        sql = db.EVENT_ODDS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["home_team_moneyline"],
            item["away_team_moneyline"],
            item["home_team_spread"],
            item["away_team_spread"],
            item["home_team_spread_odds"],
            item["away_team_spread_odds"],
            item["over_under"],
            item["over_odds"],
            item["under_odds"],
            item["spread_history_ref"],
            item["total_history_ref"],
            item["moneyline_history_ref"],
            last_updated
        )
        tx.execute(sql, args)

    
    def _spread_history_insert(self, tx, item):
        sql = db.EVENT_SPREAD_DETAILS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["home_team_spread_odds_low"],
            item["home_team_spread_odds_high"],
            item["home_team_spread_odds_open"],
            item["home_team_spread_odds_current"],
            item["home_team_spread_odds_prev"],
            item["away_team_spread_odds_low"],
            item["away_team_spread_odds_high"],
            item["away_team_spread_odds_open"],
            item["away_team_spread_odds_current"],
            item["away_team_spread_odds_prev"],
            item["movement_ref"],
            last_updated
        )
        tx.execute(sql, args)

    
    def _total_history_insert(self, tx, item):
        sql = db.EVENT_TOTAL_DETAILS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["total_line_low"],
            item["total_line_high"],
            item["total_line_open"],
            item["total_line_current"],
            item["total_line_prev"],
            item["movement_ref"],
            last_updated
        )
        tx.execute(sql, args)

    def _moneyline_history_insert(self, tx, item):
        sql = db.EVENT_MONEYLINE_DETAILS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["home_team_low"],
            item["home_team_high"],
            item["home_team_open"],
            item["home_team_current"],
            item["home_team_prev"],
            item["away_team_low"],
            item["away_team_high"],
            item["away_team_open"],
            item["away_team_current"],
            item["away_team_prev"],
            item["movement_ref"],
            last_updated
        )
        tx.execute(sql, args)
    
    def _spread_movement_insert(self, tx, item):
        sql = db.EVENT_SPREAD_MOVEMENT_UPSERT_QUERY
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["home_team_odds"],
            item["away_team_odds"],
            item["line"],
            item["timestamp"]
        )
        tx.execute(sql, args)

    def _total_movement_insert(self, tx, item):
        sql = db.EVENT_TOTAL_MOVEMENT_UPSERT_QUERY
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["over_odds"],
            item["under_odds"],
            item["total_line"],
            item["timestamp"]
        )
        tx.execute(sql, args)
    
    def _moneyline_movement_insert(self, tx, item):
        sql = db.EVENT_MONEYLINE_MOVEMENT_UPSERT_QUERY
        args = (
            item["event_id"],
            item["provider_id"],
            item["home_team_id"],
            item["away_team_id"],
            item["home_team_odds"],
            item["away_team_odds"],
            item["timestamp"]
        )
        tx.execute(sql, args)
    


    def _success(self, result, item):
        item_type = type(item).__name__
        success_logger.info(f"{item_type} successfully processed: {item}")

    def _handle_error(self, failure, item):
        exception = failure.value
        success_logger.info(f"Handling error in pipeline: {exception.args[0]} {failure} item: {item}")
        if isinstance(exception, IntegrityError):
            self.foreign_key_logger.info(f"Foreign key constraint error: {exception} item: {item}")
            # MySQL error code for foreign key violation is typically 1451 or 1452
            if exception.args[0] in (1451, 1452) and isinstance(item, EventItem):
                self._event_item_foriegn_key_handler(exception, item)

            elif exception.args[0] in (1451, 1452) and isinstance(item, AthleteItem):
                print("FOREIGN KEY ERROR YOOOOOOOOOOOOOOOOOOO: error is : "+ str(exception.args[0]))

            elif exception.args[0] in (1451, 1452) and isinstance(item, RosterItem):
                self.foreign_key_logger.info(f"calling roster foreign key handler method  {item['athlete_id']}")
                self._roster_foreign_key_handler(exception, item)
            elif exception.args [0] in (1451, 1452) and isinstance(item, EventOddsItem):
                self.foreign_key_logger.info(f"calling event odds foreign key handler method  {item['event_id']}")
                self._event_odds_foreign_key_handler(exception, item)
            
            else:
                raise DropItem(f"Error processing item: {failure} item: {item}")
        else:
            raise DropItem(f"Error processing item: {failure} item: {item}")

    def _handle_missing_college(self, team_item, event_item):
        college_item = {}
        college_item["college_espn_id"] = team_item["college_espn_id"]
        college_item["team_name"] = team_item["team_name"]
        college_item["team_location"] = None
        try:
            college_query = self.dbpool.runInteraction(self._college_insert, college_item)

            college_query.addCallback(self._handle_missing_team, team_item, event_item)
            college_query.addErrback(self._handle_error, team_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {team_item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {team_item}")



    def _handle_missing_team(self, result, team_item, event_item):
        try:
            team_query = self.dbpool.runInteraction(self._team_insert, team_item)
            team_query.addCallback(self._on_team_insert_success, team_item, event_item)
            team_query.addErrback(self._handle_error, event_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {team_item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {team_item}")

    def _on_team_insert_success(self, result, team_item, event_item):
        self.success_logger.info(f"Team successfully processed: {team_item} ")
        self.foreign_key_logger.info(f"Team successfully processed: {team_item} ")
        try:
            event_query = self.dbpool.runInteraction(self._event_insert, event_item)
            event_query.addCallback(self._on_event_insert_success, event_item)
            event_query.addErrback(self._handle_error, event_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {event_item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {event_item}")

    def _on_event_insert_success(self, result, event_item):
        self.success_logger.info(f"Event successfully processed after adding missing team: {event_item} ")
        self.foreign_key_logger.info(f"Event successfully processed after adding missing team: {event_item} ")
        print(f"Event successfully processed after adding missing team: {event_item} ")

        try:
            event_query = self.dbpool.runInteraction(self._event_details_insert, event_item)
            event_query.addErrback(self._handle_error, event_item)
            event_query.addCallback(self._success, event_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {event_item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {event_item}")


    def _event_item_foriegn_key_handler(self, exception, item):
        if "away_team" in str(exception):
            teams = item["name"].split(" at ")
            team_abbrs = item["shortname"].split(" @ ")
            team_item = {}
            team_item["team_name"] = teams[0]
            self.foreign_key_logger.info(f"Handling missing team.... "+ str(team_item["team_name"])+ ":  with id: "+ str(item["away_team_college_id"]))
            team_item["college_espn_id"] = item["away_team_college_id"]
            team_item["team_location"] = None
            team_item["team_conference"] = None
            team_item["team_abbr"] = team_abbrs[0]
            team_item["season"] = item["season"]
            team_item["team_events_ref"] = None
            team_item["conference_ref"] = None
            team_item["record_ref"]= None 
            team_item["ats_ref"] = None
            team_item["ranks_ref"] = None
        elif "home_team" in str(exception):
            teams = item["name"].split(" at ")
            team_abbrs = item["shortname"].split(" @ ")
            team_item = {}
            team_item["team_name"] = teams[1]
            self.foreign_key_logger.info(f"Handling missing team.... "+ str(team_item["team_name"])+ ":  with id: "+ str(item["home_team_college_id"]))
            team_item["college_espn_id"] = item["home_team_college_id"]
            team_item["team_location"] = None
            team_item["team_conference"] = None
            team_item["team_abbr"] = team_abbrs[1]
            team_item["season"] = item["season"]
            team_item["team_events_ref"] = None
            team_item["conference_ref"] = None
            team_item["record_ref"]= None 
            team_item["ats_ref"] = None
            team_item["ranks_ref"] = None

        self._handle_missing_college(team_item, item)


    def _roster_foreign_key_handler(self, exception, item):
        if "athlete_id" in str(exception):
            self._handle_missing_athlete(item)

        else:
            raise DropItem(f"Error processing item: {exception} item: {item}")


    def _event_odds_foreign_key_handler(self, exception, item):
        if "provider_id" in str(exception):
            self._handle_missing_bookie(item)
        else:
            raise DropItem(f"Error processing item: {exception} item: {item}")
        
    def _handle_missing_bookie(self, item):
        try:
            bookie_item = {}
            bookie_item["id"] = item["provider_id"]
            bookie_item["name"] = None
            bookie_item["bookie_ref"] = f"http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/providers/{item['provider_id']}?lang=en&region=us"
            bookie_item["priority"] = 0
            bookie_query = self.dbpool.runInteraction(self._bookie_insert, bookie_item)
            bookie_query.addCallback(self._on_bookie_reinsert_success, item)
            bookie_query.addErrback(self._handle_error)
        except Exception as e:
            self.foreign_key_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.error(f"Error processing item: {e} item: {item}")
    
    def _handle_missing_athlete(self, item):
        try:
            athlete_item = {}
            athlete_item["id"] = item["athlete_id"]
            athlete_item["first_name"] = None
            athlete_item["last_name"] = None
            athlete_item["name"] = None
            athlete_item["birthplace"] = None
            athlete_item["athlete_ref"] = item["athlete_ref"]
            team_query = self.dbpool.runInteraction(self._athlete_insert, athlete_item)
            team_query.addCallback(self._on_athlete_reinsert_success, item)
            team_query.addErrback(self._handle_error)
        except Exception as e:
            self.foreign_key_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.error(f"Error processing item: {e} item: {item}")

    def _on_athlete_reinsert_success(self, result, item):
        self.success_logger.info(f"Athlete successfully processed: {item} ")
        self.foreign_key_logger.info(f"Athlete successfully inserted into db now retrying roster insert: {item['athlete_id']} ")
        try:
            roster_query = self.dbpool.runInteraction(self._roster_insert, item)
            roster_query.addCallback(self._success, item)
            roster_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")

    def _on_bookie_reinsert_success(self, result, item):
        self.success_logger.info(f"Bookie successfully processed: {item} ")
        self.foreign_key_logger.info(f"Bookie successfully inserted into db now retrying event odds insert: {item['provider_id']} ")
        try:    
            event_odds_query = self.dbpool.runInteraction(self._event_odds_insert, item)
            event_odds_query.addCallback(self._success, item)
            event_odds_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")

    def _conference_success(self, result, item):
        self.success_logger.info(f"Conference successfully processed: {item} ")
        self.success_logger.info(f"Processing corresponding conference details")
        try:
            conf_details_item = {}
            conf_details_item["conference_id"] = item["conference_id"]
            conf_details_item["name"] = item["name"]
            conf_details_item["short_name"] = item["short_name"]
            conf_details_item["conference_standings_ref"] = item["conference_standings_ref"]
            conf_details_item["conference_teams_ref"] = item["conference_teams_ref"]
            conf_details_item["season"] = item["season"]

            conf_details_query = self.dbpool.runInteraction(self._conference_details_insert, conf_details_item)
            conf_details_query.addCallback(self._success, item)
            conf_details_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")



    def _college_success(self, result, item):
        self.success_logger.info(f"College successfully processed: {item} ")
        self.success_logger.info(f"Processing corresponding team")
        try:
            team_query = self.dbpool.runInteraction(self._team_insert, item)
            team_query.addCallback(self._success, item)
            team_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")


    def _athlete_success(self, result, item):
        self.success_logger.info(f"Athlete successfully processed: {item} ")
        self.success_logger.info(f"Processing corresponding athlete details")
        try:
            athlete_details_query = self.dbpool.runInteraction(self._athlete_details_insert, item)
            athlete_details_query.addCallback(self._success, item)
            athlete_details_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")

    def _venue_success(self, result, item):
        self.success_logger.info(f"venue successfully processed : {item}")
        self.success_logger.info("processing corressponding event")
        try:
            event_query = self.dbpool.runInteraction(self._event_insert, item)
            event_query.addCallback(self._event_success, item)
            event_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item {e} item : {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")
    
    def _event_success(self, result, item):
        self.success_logger.info(f"event successfully processed : {item}")
        self.success_logger.info("processing corressponding event details")
        try:
            event_query = self.dbpool.runInteraction(self._event_details_insert, item)
            event_query.addCallback(self._success, item)
            event_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item {e} item : {item}")
            self.foreign_key_logger.info(f"Error processing item: {e} item: {item}")
        
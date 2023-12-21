# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from collegebball.items import (
    CollegeItem,
    ConferenceDetailsItem,
    ConferenceItem,
    TeamItem,
    TeamOutcomesItem,
    EventItem,
    EventDetailsItem,
    VenueItem,
    AthleteItem,
    AthleteDetailsItem,
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
foreign_key_logger.setLevel(logging.ERROR)
foreign_key_log_handler = logging.FileHandler(
    "foreign_keys.log", mode="w"
)  # Overwrite the log file for foreign key errors
foreign_key_log_handler.setLevel(logging.ERROR)
foreign_key_logger.addHandler(foreign_key_log_handler)

event_reinsert_logger = logging.getLogger("event_reinsert")
event_reinsert_logger.setLevel(logging.INFO)
event_reinsert_log_handler = logging.FileHandler(
    "event_reinsert.log", mode="w"
)  # Overwrite the log file for foreign key errors
event_reinsert_log_handler.setLevel(logging.INFO)
event_reinsert_logger.addHandler(event_reinsert_log_handler)

athlete_details_reinsert_logger = logging.getLogger("athlete_details_reinsert")
athlete_details_reinsert_logger.setLevel(logging.INFO)
athlete_details_reinsert_log_handler = logging.FileHandler(
    "athlete_details_reinsert.log", mode="w"
)  # Overwrite the log file for foreign key errors
athlete_details_reinsert_log_handler.setLevel(logging.INFO)
athlete_details_reinsert_logger.addHandler(athlete_details_reinsert_log_handler)



class MySQLStorePipeline(object):
    def __init__(self):
        self.dbpool = db.get_db_pool()
        self.success_logger = success_logger
        self.error_logger = error_logger
        self.foreign_key_logger = foreign_key_logger
        self.event_reinsert_logger = event_reinsert_logger
        self.athlete_details_reinsert_logger = athlete_details_reinsert_logger

    def process_item(self, item, spider):
        success_logger.info(f"Processing items in pipeline")
        if isinstance(item, CollegeItem):
            success_logger.info(f"Processing college item")
            if (
                item["college_espn_id"] is not None
                and item["team_name"] is not None
                and item["team_location"] is not None
            ):
                college_query = self.dbpool.runInteraction(self._college_insert, item)
                college_query.addErrback(self._handle_error, item)
                self.success_logger.info(f"College successfully processed: {item} ")
            else:
                self.error_logger.info(f"{item} has no college_espn_id")
                raise DropItem(f"Missing college_espn_id in {item}")

        elif isinstance(item, TeamItem):
            success_logger.info(f"Processing team item")
            team_query = self.dbpool.runInteraction(self._team_insert, item)
            team_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Team successfully processed: {item} ")

        elif isinstance(item, ConferenceItem):
            success_logger.info(f"Processing conference item")
            conf_query = self.dbpool.runInteraction(self._conference_insert, item)
            conf_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Conference successfully processed: {item}")

        elif isinstance(item, TeamOutcomesItem):
            success_logger.info(f"Processing team outcomes item")
            record_query = self.dbpool.runInteraction(self._record_insert, item)
            record_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Team outcomes successfully processed: {item}")

        elif isinstance(item, ConferenceDetailsItem):
            success_logger.info(f"Processing conference details item")
            conf_details_query = self.dbpool.runInteraction(
                self._conference_details_insert, item
            )
            conf_details_query.addErrback(self._handle_error, item)
            self.success_logger.info(
                f"Conference details successfully processed: {item}"
            )

        elif isinstance(item, EventItem):
            success_logger.info(f"Processing event item")
            event_query = self.dbpool.runInteraction(self._event_insert, item)
            event_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Event successfully processed: {item}")

        elif isinstance(item, EventDetailsItem):
            success_logger.info(f"Processing event details item")
            event_details_query = self.dbpool.runInteraction(
                self._event_details_insert, item
            )
            event_details_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Event details successfully processed: {item}")

        elif isinstance(item, VenueItem):
            success_logger.info(f"Processing venue item")
            venue_query = self.dbpool.runInteraction(self._venue_insert, item)
            venue_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Venue successfully processed: {item}")

        elif isinstance(item, AthleteItem):
            success_logger.info(f"Processing athlete item")
            athlete_query = self.dbpool.runInteraction(self._athlete_insert, item)
            athlete_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"Athlete successfully processed: {item}")

        elif isinstance(item, AthleteDetailsItem):
            success_logger.info(f"Processing athlete details item")
            athleteDetails_query = self.dbpool.runInteraction(
                self._athlete_details_insert, item
            )
            athleteDetails_query.addErrback(self._handle_error, item)
            self.success_logger.info(f"athlete details successfully processed: {item}")
        return item

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
            item["name"],
            item["city"],
            item["state"],
            item["capacity"],
            item["indoor"],
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
        )
        tx.execute(sql, args)

    def _athlete_details_insert(self, tx, item):
        sql = db.ATHLETE_DETAILS_UPSERT_QUERY
        last_updated = datetime.datetime.now()
        args = (
            item["athlete_id"],
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

    def _handle_error(self, failure, item):
        exception = failure.value
        success_logger.info(
            f"Handling error in pipeline: {exception.args[0]} {failure} item: {item}"
        )
        if isinstance(exception, IntegrityError):
            self.foreign_key_logger.info(
                f"Foreign key constraint error: {exception} item: {item}"
            )
            # MySQL error code for foreign key violation is typically 1451 or 1452
            if exception.args[0] in (1451, 1452) and isinstance(item, EventItem):
                # Handling specific case for teams
                if "away_team" in str(exception):
                    teams = item["name"].split(" at ")
                    team_abbrs = item["shortname"].split(" @ ")
                    team_item = {}
                    team_item["team_name"] = teams[0]
                    self.event_reinsert_logger.info(
                        f"Handling missing team.... "
                        + str(
                            team_item["team_name"]
                            + ":  with id: "
                            + str(item["away_team_college_id"])
                        )
                    )
                    team_item["college_espn_id"] = item["away_team_college_id"]
                    team_item["team_location"] = None
                    team_item["team_conference"] = None
                    team_item["team_abbr"] = team_abbrs[0]
                    team_item["season"] = item["season"]
                    team_item["team_events_ref"] = None
                elif "home_team" in str(exception):
                    teams = item["name"].split(" at ")
                    team_abbrs = item["shortname"].split(" @ ")
                    team_item = {}
                    team_item["team_name"] = teams[1]
                    self.event_reinsert_logger.info(
                        f"Handling missing team.... "
                        + str(
                            team_item["team_name"]
                            + ":  with id: "
                            + str(item["home_team_college_id"])
                        )
                    )
                    team_item["college_espn_id"] = item["home_team_college_id"]
                    team_item["team_location"] = None
                    team_item["team_conference"] = None
                    team_item["team_abbr"] = team_abbrs[1]
                    team_item["season"] = item["season"]
                    team_item["team_events_ref"] = None

                self._handle_missing_college(team_item, item)
            elif exception.args[0] in (1451, 1452) and isinstance(item, AthleteDetailsItem):
                if "team_id" in str(exception):
                    self.athlete_details_reinsert_logger.info(
                        f"Handling missing team.... "
                        + str(
                            item["athlete_id"]
                            + ":  with id: "
                            + str(item["team_id"]) + " and season: " + str(item["season"])
                        )
                    )
                    self._handle_missing_athlete_team(item)
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
            college_query = self.dbpool.runInteraction(
                self._college_insert, college_item
            )

            college_query.addCallback(
                self._on_college_insert_success, team_item, event_item
            )
            college_query.addErrback(self._handle_error, team_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {team_item}")
            self.event_reinsert_logger.error(
                f"Error processing item: {e} item: {team_item}"
            )

    def _on_college_insert_success(self, _result, team_item, event_item):
        self.success_logger.info(f"college successfully processed: {team_item} ")
        self.event_reinsert_logger.info(f"college successfully processed: {team_item}")
        self._handle_missing_team(team_item, event_item)

    def _handle_missing_team(self, team_item, event_item):
        try:
            team_query = self.dbpool.runInteraction(self._team_insert, team_item)
            team_query.addCallback(self._on_team_insert_success, team_item, event_item)
            team_query.addErrback(self._handle_error, event_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {team_item}")
            self.event_reinsert_logger.error(
                f"Error processing item: {e} item: {team_item}"
            )

    def _on_team_insert_success(self, result, team_item, event_item):
        self.success_logger.info(f"Team successfully processed: {team_item} ")
        self.event_reinsert_logger.info(f"Team successfully processed: {team_item} ")
        try:
            event_query = self.dbpool.runInteraction(self._event_insert, event_item)
            event_query.addCallback(self._on_event_insert_success, event_item)
            event_query.addErrback(self._handle_error, event_item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {event_item}")
            self.event_reinsert_logger.error(
                f"Error processing item: {e} item: {event_item}"
            )

    def _on_event_insert_success(self, result, event_item):
        self.success_logger.info(
            f"Event successfully processed after adding missing team: {event_item} "
        )
        self.event_reinsert_logger.info(
            f"Event successfully processed after adding missing team: {event_item} "
        )

    def _handle_missing_athlete_team(self, result, item):
        sql = """
                INSERT INTO teams (espn_id, location, name, season)
                SELECT espn_id, location, name, %s
                FROM colleges
                WHERE espn_id = %s;
            """

        ath_team_query = self.dbpool.runInteraction(
            self._athlete_team_insert, item, sql
        )
        ath_team_query.addCallback(self._on_athlete_team_insert_success, item)
        ath_team_query.addErrback(self._handle_error, item)

    def _on_athlete_team_insert_success(self, result, item):
        self.success_logger.info(f"Missing team successfully processed ")
        self.athlete_details_reinsert_logger.info(
            f"Team successfully processed after adding missing team: {item} "
        )
        try:
            athlete_details_query = self.dbpool.runInteraction(
                self._athlete_details_insert, item
            )
            athlete_details_query.addCallback(
                self._on_athlete_details_insert_success, item
            )
            athlete_details_query.addErrback(self._handle_error, item)
        except Exception as e:
            self.error_logger.error(f"Error processing item: {e} item: {item}")
            self.athlete_details_reinsert_logger.error(
                f"Error processing item: {e} item: {item}"
            )

    def _on_athlete_details_insert_success(self, result, item):
        self.success_logger.info(
            f"Athlete details successfully processed after adding missing team: {item} "
        )
        self.athlete_details_reinsert_logger.info(
            f"Athlete details successfully processed after adding missing team: {item} "
        )
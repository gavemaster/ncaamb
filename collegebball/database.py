import MySQLdb
import os
from twisted.enterprise import adbapi
from dotenv import load_dotenv

load_dotenv()

COLLEGE_INSERT_QUERY = """INSERT IGNORE INTO colleges (college_id, name, location)
                    VALUES (%s, %s, %s) """

TEAM_UPSERT_QUERY = """INSERT INTO teams (team_name, team_college_id, location, conference, team_abbr, team_season, events_link, conference_link, record_link, 
                                            ats_link, ranks_link)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s)
                    ON DUPLICATE KEY UPDATE
                    name = VALUES(team_name),
                    location = VALUES(location),
                    conference = VALUES(conference),
                    team_abbr = VALUES(team_abbr),
                    events_link = VALUES(events_link),
                    conference_link = VALUES(conference_link),
                    record_link = VALUES(record_link),
                    ats_link = VALUES(ats_link),
                    ranks_link = VALUES(ranks_link);
                    """

TEAM_RESULTS_UPSERT_QUERY = """
        INSERT INTO team_results (results_college_id, results_season, ats_overall_wins, ats_overall_losses, ats_overall_pushes, ats_favorite_wins, ats_favorite_losses, ats_favorite_pushes, ats_underdog_wins, 
                                    ats_underdog_losses, ats_underdog_pushes, ats_away_wins, ats_away_losses, ats_away_pushes, ats_home_wins, ats_home_losses, ats_home_pushes, ats_away_favorite_wins, 
                                    ats_away_favorite_losses, ats_away_favorite_pushes, ats_away_underdog_wins, ats_away_underdog_losses, ats_away_underdog_pushes, ats_home_favorite_wins, ats_home_favorite_losses,
                                    ats_home_favorite_pushes, ats_home_underdog_wins, ats_home_underdog_losses, ats_home_underdog_pushes, wins, losses, overtime_wins, overtime_losses, point_differential, 
                                    average_points, average_points_allowed, division_win_percentage, games_played, home_wins, home_losses, home_overtime_wins, home_overtime_losses, home_point_differential, 
                                    home_average_points, home_average_points_allowed, home_divsion_win_percentage, home_games_played, away_wins, away_losses, away_overtime_wins, away_overtime_losses, away_point_differential, 
                                    away_average_points, away_average_points_allowed, away_division_win_percentage, away_games_played, ap_top_25_wins, ap_top_25_losses, ap_top_25_point_differential, ap_top_25_average_points, 
                                    ap_top_25_average_points_allowed, ap_top_25_games_played, conference_wins, conference_losses, conference_overtime_wins, conference_overtime_losses, conference_point_differential, 
                                    conference_average_points, conference_average_points_allowed, conference_games_played, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s)
                    ON DUPLICATE KEY UPDATE
                    ats_overall_wins = VALUES(ats_overall_wins),
                    ats_overall_losses = VALUES(ats_overall_losses),
                    ats_overall_pushes = VALUES(ats_overall_pushes),
                    ats_favorite_wins = VALUES(ats_favorite_wins),
                    ats_favorite_losses = VALUES(ats_favorite_losses),
                    ats_favorite_pushes = VALUES(ats_favorite_pushes),
                    ats_underdog_wins = VALUES(ats_underdog_wins),
                    ats_underdog_losses = VALUES(ats_underdog_losses),
                    ats_underdog_pushes = VALUES(ats_underdog_pushes),
                    ats_away_wins = VALUES(ats_away_wins),
                    ats_away_losses = VALUES(ats_away_losses),
                    ats_away_pushes = VALUES(ats_away_pushes),
                    ats_home_wins = VALUES(ats_home_wins),
                    ats_home_losses = VALUES(ats_home_losses),
                    ats_home_pushes = VALUES(ats_home_pushes),
                    ats_away_favorite_wins = VALUES(ats_away_favorite_wins),
                    ats_away_favorite_losses = VALUES(ats_away_favorite_losses),
                    ats_away_favorite_pushes = VALUES(ats_away_favorite_pushes),
                    ats_away_underdog_wins = VALUES(ats_away_underdog_wins),
                    ats_away_underdog_losses = VALUES(ats_away_underdog_losses),
                    ats_away_underdog_pushes = VALUES(ats_away_underdog_pushes),
                    ats_home_favorite_wins = VALUES(ats_home_favorite_wins),
                    ats_home_favorite_losses = VALUES(ats_home_favorite_losses),
                    ats_home_favorite_pushes = VALUES(ats_home_favorite_pushes),
                    ats_home_underdog_wins = VALUES(ats_home_underdog_wins),
                    ats_home_underdog_losses = VALUES(ats_home_underdog_losses),
                    ats_home_underdog_pushes = VALUES(ats_home_underdog_pushes),
                    wins = VALUES(wins),
                    losses = VALUES(losses),
                    overtime_wins = VALUES(overtime_wins),
                    overtime_losses = VALUES(overtime_losses),
                    point_differential = VALUES(point_differential),
                    average_points = VALUES(average_points),
                    average_points_allowed = VALUES(average_points_allowed),
                    division_win_percentage = VALUES(division_win_percentage),
                    games_played = VALUES(games_played),
                    home_wins = VALUES(home_wins),
                    home_losses = VALUES(home_losses),
                    home_overtime_wins = VALUES(home_overtime_wins),
                    home_overtime_losses = VALUES(home_overtime_losses),
                    home_point_differential = VALUES(home_point_differential),
                    home_average_points = VALUES(home_average_points),
                    home_average_points_allowed = VALUES(home_average_points_allowed),
                    home_divsion_win_percentage = VALUES(home_divsion_win_percentage),
                    home_games_played = VALUES(home_games_played),
                    away_wins = VALUES(away_wins),
                    away_losses = VALUES(away_losses),
                    away_overtime_wins = VALUES(away_overtime_wins),
                    away_overtime_losses = VALUES(away_overtime_losses),
                    away_point_differential = VALUES(away_point_differential),
                    away_average_points = VALUES(away_average_points),
                    away_average_points_allowed = VALUES(away_average_points_allowed),
                    away_division_win_percentage = VALUES(away_division_win_percentage),
                    away_games_played = VALUES(away_games_played),
                    ap_top_25_wins = VALUES(ap_top_25_wins),
                    ap_top_25_losses = VALUES(ap_top_25_losses),
                    ap_top_25_point_differential = VALUES(ap_top_25_point_differential),
                    ap_top_25_average_points = VALUES(ap_top_25_average_points),
                    ap_top_25_average_points_allowed = VALUES(ap_top_25_average_points_allowed),
                    ap_top_25_games_played = VALUES(ap_top_25_games_played),
                    conference_wins = VALUES(conference_wins),
                    conference_losses = VALUES(conference_losses),
                    conference_overtime_wins = VALUES(conference_overtime_wins),
                    conference_overtime_losses = VALUES(conference_overtime_losses),
                    conference_point_differential = VALUES(conference_point_differential),
                    conference_average_points = VALUES(conference_average_points),
                    conference_average_points_allowed = VALUES(conference_average_points_allowed),
                    conference_games_played = VALUES(conference_games_played),
                    last_updated = VALUES(last_updated);
        
        """


CONFERENCE_INSERT_QUERY = """INSERT IGNORE INTO conferences (conference_id, conference_name)
                    VALUES (%s, %s) """


CONFERENCE_DETAILS_UPSERT_QUERY = """INSERT INTO conference_details (details_conference_id, name, short_name, standings_ref, teams_ref, season, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    short_name = VALUES(short_name),
                    standings_ref = VALUES(standings_ref),
                    teams_ref = VALUES(teams_ref),
                    last_updated = VALUES(last_updated);"""


EVENT_UPSERT_QUERY = """
        INSERT INTO events (event_id, event_name, event_short_name, event_date, event_time, event_venue_id, home_team_college_id, away_team_college_id, 
                                home_team_score, away_team_score, event_season, event_season_type, event_week, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                name = VALUES(event_name),
                                short_name = VALUES(event_short_name),
                                date = VALUES(event_date),
                                time = VALUES(event_time),
                                venue_id = VALUES(event_venue_id),
                                home_team_college_id = VALUES(home_team_college_id),
                                away_team_college_id = VALUES(away_team_college_id),
                                home_team_score = VALUES(home_team_score),
                                away_team_score = VALUES(away_team_score),
                                season = VALUES(event_season),
                                season_type = VALUES(event_season_type),
                                week = VALUES(event_week),
                                last_updated = VALUES(last_updated);
"""

EVENT_DETAILS_UPSERT_QUERY = """
        INSERT INTO event_details (details_event_id, odds_ref, neutral_site, division_game, conference_game, home_team_stats_ref, away_team_stats_ref, home_team_roster_ref, away_team_roster_ref,
                                        event_details_ref, event_ref, status, home_team_rank, away_team_rank, home_team_win, away_team_win, home_team_record_ref, away_team_record_ref, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                odds_ref = VALUES(odds_ref),
                                neutral_site = VALUES(neutral_site),
                                division_game = VALUES(division_game),
                                conference_game = VALUES(conference_game),
                                home_team_stats_ref = VALUES(home_team_stats_ref),
                                away_team_stats_ref = VALUES(away_team_stats_ref),
                                home_team_roster_ref = VALUES(home_team_roster_ref),
                                away_team_roster_ref = VALUES(away_team_roster_ref),
                                event_details_ref = VALUES(event_details_ref),
                                event_ref = VALUES(event_ref),
                                status = VALUES(status),
                                home_team_rank = VALUES(home_team_rank),
                                away_team_rank = VALUES(away_team_rank),
                                home_team_win = VALUES(home_team_win),
                                away_team_win = VALUES(away_team_win),
                                home_team_record_ref = VALUES(home_team_record_ref),
                                away_team_record_ref = VALUES(away_team_record_ref),
                                last_updated = VALUES(last_updated);

        
        
        
"""


VENUE_UPSERT_QUERY = """
        INSERT INTO venues (venue_id, name, venue_city, venue_state, capacity, indoor, venue_ref, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                name = VALUES(name),
                                city = VALUES(venue_city),
                                state = VALUES(venue_state),
                                capacity = VALUES(capacity),
                                indoor = VALUES(indoor),
                                venue_ref = VALUES(venue_ref),
                                last_updated = VALUES(last_updated);

"""


ATHLETE_UPSERT_QUERY = """
        INSERT IGNORE INTO athletes (athlete_id, first_name, last_name, full_name, birthplace, athlete_ref, last_updated, headshot)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

ATHLETE_DETAILS_UPSERT_QUERY = """
        INSERT INTO athlete_details (athlete_id, season, team_id, jersey_number, position, height, weight, exp, class_year, status, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s)
                        ON DUPLICATE KEY UPDATE
                                team_id = VALUES(team_id),
                                jersey_number = VALUES(jersey_number),
                                position = VALUES(position),
                                height = VALUES(height),
                                weight = VALUES(weight),
                                exp = VALUES(exp),
                                class_year = VALUES(class_year),
                                status = VALUES(status),
                                last_updated = VALUES(last_updated);

"""
ROSTER_UPSERT_QUERY = """
        INSERT INTO event_rosters (roster_team_id, roster_athlete_id, roster_season, roster_event_id, did_not_play, stats_ref, starter, ejected, last_updated, athlete_ref)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                did_not_play = VALUES(did_not_play),
                                stats_ref = VALUES(stats_ref),
                                starter = VALUES(starter),
                                ejected = VALUES(ejected),
                                last_updated = VALUES(last_updated),
                                athlete_ref = VALUES(athlete_ref);
"""


PLAYER_STATS_UPSERT_QUERY = """
    INSERT INTO player_stats (player_stats_athlete_id, player_stats_team_id, player_stats_season, player_stats_event_id, blks, dreb, stls, points_off_turnovers, flagrant_fouls, fouls, ejections, tech_fouls, tot_reb, minutes, fantasy_rating, plus_minus,
													ast_to_ratio, stl_foul_ratio, blk_foul_ratio, stl_to_ratio, games_played, games_started, double_double, triple_double, ast, fga, fgm, fg_pct, fta, ftm, ft_pct, oreb,
													pts, turnovers, 3fga, 3fgm, 3fg_pct, second_chance_pts, fast_break_pts, oreb_pct, 2fga, 2fgm, 2fg_pct, shooting_eff, scoring_eff, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
								%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
								%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
								%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
								%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                blks = VALUES(blks),
                                dreb = VALUES(dreb),
                                stls = VALUES(stls),
                                points_off_turnovers = VALUES(points_off_turnovers),
                                flagrant_fouls = VALUES(flagrant_fouls),
                                fouls = VALUES(fouls),
                                ejections = VALUES(ejections),
                                tech_fouls = VALUES(tech_fouls),
                                tot_reb = VALUES(tot_reb),
                                minutes = VALUES(minutes),
                                fantasy_rating = VALUES(fantasy_rating),
                                plus_minus = VALUES(plus_minus),
                                ast_to_ratio = VALUES(ast_to_ratio),
                                stl_foul_ratio = VALUES(stl_foul_ratio),
                                blk_foul_ratio = VALUES(blk_foul_ratio),
                                stl_to_ratio = VALUES(stl_to_ratio),
                                games_played = VALUES(games_played),
                                games_started = VALUES(games_started),
                                double_double = VALUES(double_double),
                                triple_double = VALUES(triple_double),
                                ast = VALUES(ast),
                                fga = VALUES(fga),
                                fgm = VALUES(fgm),
                                fg_pct = VALUES(fg_pct),
                                fta = VALUES(fta),
                                ftm = VALUES(ftm),
                                ft_pct = VALUES(ft_pct),
                                oreb = VALUES(oreb),
                                pts = VALUES(pts),
                                turnovers = VALUES(turnovers),
                                3fga = VALUES(3fga),
                                3fgm = VALUES(3fgm),
                                3fg_pct = VALUES(3fg_pct),
                                second_chance_pts = VALUES(second_chance_pts),
                                fast_break_pts = VALUES(fast_break_pts),
                                oreb_pct = VALUES(oreb_pct),
                                2fga = VALUES(2fga),
                                2fgm = VALUES(2fgm),
                                2fg_pct = VALUES(2fg_pct),
                                shooting_eff = VALUES(shooting_eff),
                                scoring_eff = VALUES(scoring_eff),
                                last_updated = VALUES(last_updated);
                                
"""

BOOKIE_UPSERT_QUERY = """
                        INSERT INTO odd_providers(provider_id, provider_name, provider_ref, provider_priority)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                name = VALUES(provider_name),
                                ref = VALUES(provider_ref),
                                priority = VALUES(provider_priority);
"""


EVENT_ODDS_UPSERT_QUERY = """
                        INSERT INTO event_odds(event_id, provider_id, home_team_id, away_team_id, home_team_moneyline, away_team_moneyline,
                                        home_team_spread, away_team_spread, home_team_spread_odds, away_team_spread_odds, total_line, over_odds,
                                        under_odds, spread_details_ref, total_details_ref, moneyline_details_ref, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                home_team_moneyline = VALUES(home_team_moneyline),
                                away_team_moneyline = VALUES(away_team_moneyline),
                                home_team_spread = VALUES(home_team_spread),
                                away_team_spread = VALUES(away_team_spread),
                                home_team_spread_odds = VALUES(home_team_spread_odds),
                                away_team_spread_odds = VALUES(away_team_spread_odds),
                                total_line = VALUES(total_line),
                                over_odds = VALUES(over_odds),
                                under_odds = VALUES(under_odds),
                                spread_details_ref = VALUES(spread_details_ref),
                                total_details_ref = VALUES(total_details_ref),
                                moneyline_details_ref = VALUES(moneyline_details_ref),
                                last_updated = VALUES(last_updated);
"""


EVENT_SPREAD_DETAILS_UPSERT_QUERY = """
                        INSERT INTO spread_odds_details(event_id, provider_id, home_team_id, away_team_id, home_team_spread_odds_low,
                                        home_team_spread_odds_high, home_team_spread_odds_open, home_team_spread_odds_current,
                                        home_team_spread_odds_previous, away_team_spread_odds_low, away_team_spread_odds_high,
                                        away_team_spread_odds_open, away_team_spread_odds_current, away_team_spread_odds_previous,
                                        movement_ref, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                home_team_spread_odds_low = VALUES(home_team_spread_odds_low),
                                home_team_spread_odds_high = VALUES(home_team_spread_odds_high),
                                home_team_spread_odds_open = VALUES(home_team_spread_odds_open),
                                home_team_spread_odds_current = VALUES(home_team_spread_odds_current),
                                home_team_spread_odds_previous = VALUES(home_team_spread_odds_previous),
                                away_team_spread_odds_low = VALUES(away_team_spread_odds_low),
                                away_team_spread_odds_high = VALUES(away_team_spread_odds_high),
                                away_team_spread_odds_open = VALUES(away_team_spread_odds_open),
                                away_team_spread_odds_current = VALUES(away_team_spread_odds_current),
                                away_team_spread_odds_previous = VALUES(away_team_spread_odds_previous),
                                movement_ref = VALUES(movement_ref),
                                last_updated = VALUES(last_updated);
"""

EVENT_SPREAD_MOVEMENT_UPSERT_QUERY = """
                        INSERT INTO spread_lines(event_id, provider_id, home_team_id, away_team_id, home_odds, away_odds,
                                        line, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, 
                                %s, %s)
                        ON DUPLICATE KEY UPDATE
                                home_odds = VALUES(home_odds),
                                away_odds = VALUES(away_odds),
                                line = VALUES(line);
"""

EVENT_TOTAL_DETAILS_UPSERT_QUERY = """
                        INSERT INTO total_odds_details(event_id, provider_id, home_team_id, away_team_id, total_low, total_high, 
                                        total_open, total_current, total_previous, movement_ref, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                total_low = VALUES(total_low),
                                total_high = VALUES(total_high),
                                total_open = VALUES(total_open),
                                total_current = VALUES(total_current),
                                total_previous = VALUES(total_previous),
                                movement_ref = VALUES(movement_ref),
                                last_updated = VALUES(last_updated);
"""

EVENT_TOTAL_MOVEMENT_UPSERT_QUERY = """
                        INSERT INTO total_lines(event_id, provider_id, home_team_id, away_team_id, over_odds, under_odds, line, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s)
                        ON DUPLICATE KEY UPDATE
                                over_odds = VALUES(over_odds),
                                under_odds = VALUES(under_odds),
                                line = VALUES(line);
"""

EVENT_MONEYLINE_DETAILS_UPSERT_QUERY = """
                        INSERT INTO moneyline_odds_details(event_id, provider_id, home_team_id, away_team_id, home_team_low,
                                        home_team_high, home_team_open, home_team_current, home_team_previous, away_team_low,
                                        away_team_high, away_team_open, away_team_current, away_team_previous, movement_ref, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                                home_team_low = VALUES(home_team_low),
                                home_team_high = VALUES(home_team_high),
                                home_team_open = VALUES(home_team_open),
                                home_team_current = VALUES(home_team_current),
                                home_team_previous = VALUES(home_team_previous),
                                away_team_low = VALUES(away_team_low),
                                away_team_high = VALUES(away_team_high),
                                away_team_open = VALUES(away_team_open),
                                away_team_current = VALUES(away_team_current),
                                away_team_previous = VALUES(away_team_previous),
                                movement_ref = VALUES(movement_ref),
                                last_updated = VALUES(last_updated);
"""

EVENT_MONEYLINE_MOVEMENT_UPSERT_QUERY = """
                        INSERT INTO moneyline_lines(event_id, provider_id, home_team_id, away_team_id, home_odds, away_odds, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s)
                        ON DUPLICATE KEY UPDATE
                                home_odds = VALUES(home_odds),
                                away_odds = VALUES(away_odds);
""" 

def get_db_pool():
    """Get a connection to the database"""
    return adbapi.ConnectionPool(
        "MySQLdb",
        host=os.getenv("DB_HOST_NAME"),
        db=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER_NAME"),
        passwd=os.getenv("DB_PASSWORD"),
        charset="utf8",
        use_unicode=True,
    )


def get_connection():
    """Get a connection to the database"""
    return MySQLdb.connect(
        host=os.getenv("DB_HOST_NAME"),
        db=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER_NAME"),
        passwd=os.getenv("DB_PASSWORD"),
        charset="utf8",
        use_unicode=True,
    )


def get_team_event_links_by_season_range(start, end):
    """Get the team event links for a given season range"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT events_link FROM teams WHERE season BETWEEN %s AND %s", (start, end)
    )
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    # makre results a list of strings
    results_list = [result[0] for result in results]

    # remove all None values
    results_list = [result for result in results_list if result is not None]
    return results_list


def insert_season_links_to_db(data):
    conn = get_connection()
    cursor = conn.cursor()

    query = "INSERT INTO seasons (season, season_ref) VALUES (%s, %s)"

    try:
        cursor.executemany(query, data)
        conn.commit()
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        return True


def get_season_links_from_db(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT season_ref FROM seasons WHERE season BETWEEN %s AND %s",
            (start, end),
        )
        results = cursor.fetchall()
        # make results a list of the links
        results_list = [result[0] for result in results]
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_week_links_from_db(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT weeks_ref FROM season_types WHERE year BETWEEN %s AND %s",
            (start, end),
        )
        results = cursor.fetchall()
        # make results a list of the links
        results_list = [result[0] for result in results]
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def bulk_insert_season_details_links(data):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """INSERT INTO season_details (season, name, start_date, end_date, futures_ref, leaders_ref, power_index_leaders_ref, power_indexes_ref, rankings_ref, athletes_ref)
            VALUES (%(season)s, %(display_name)s, %(start_date)s, %(end_date)s, %(futures_ref)s, %(leaders_ref)s, %(powerIndexLeaders_ref)s, %(powerIndexes_ref)s, %(rankings_ref)s, %(athletes_ref)s)
            ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            start_date = VALUES(start_date),
            end_date = VALUES(end_date),
            futures_ref = VALUES(futures_ref),
            leaders_ref = VALUES(leaders_ref),
            power_index_leaders_ref = VALUES(power_index_leaders_ref),
            power_indexes_ref = VALUES(power_indexes_ref),
            rankings_ref = VALUES(rankings_ref),
            athletes_ref = VALUES(athletes_ref)""",
            data,
        )
        conn.commit()
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        return True


def bulk_insert_season_types_links(data):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """INSERT INTO season_types (season_type_id, year, name, start_date, end_date, ref, weeks_ref)
            VALUES (%(type)s, %(year)s, %(name)s, %(start_date)s, %(end_date)s, %(ref)s, %(weeks_ref)s)
            ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            start_date = VALUES(start_date),
            end_date = VALUES(end_date),
            ref = VALUES(ref),
            weeks_ref = VALUES(weeks_ref)""",
            data,
        )
        conn.commit()
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        return True


def bulk_insert_week_details_to_db(data):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """INSERT INTO weeks (season, season_type, week, start_date, end_date, rankings_ref, name, week_events_ref)
            VALUES (%(season)s, %(type)s, %(week)s, %(start_date)s, %(end_date)s, %(rankings_ref)s, %(name)s, %(week_events_ref)s)
            ON DUPLICATE KEY UPDATE
            start_date = VALUES(start_date),
            end_date = VALUES(end_date),
            name = VALUES(name),
            rankings_ref = VALUES(rankings_ref),
            week_events_ref = VALUES(week_events_ref)""",
            data,
        )
        conn.commit()
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        return True


def get_event_landing_pages_from_db(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT season, season_type, week, week_events_ref FROM weeks WHERE (season BETWEEN %s AND %s AND (season_type = 2 OR season_type = 3))",
            (start, end),
        )
        results = cursor.fetchall()
        # make results a list of dicts
        results_list = []
        for result in results:
            results_list.append(
                {
                    "season": result[0],
                    "season_type": result[1],
                    "week": result[2],
                    "week_events_ref": result[3],
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_athlete_ids_from_db():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM athletes")
        results = cursor.fetchall()
        # make results a list of the links
        results_list = [result[0] for result in results]
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_athlete_landing_pages(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    results_list = []
    try:
        cursor.execute("SELECT athletes_ref FROM season_details WHERE season BETWEEN %s AND %s",(start, end),)
        results = cursor.fetchall()
        # make results a list of the links
        for result in results:
            results_list.append(
                {
                    "athletes_ref": result[0],
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_event_rosters(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    results_list = []
    try:
        cursor.execute("""SELECT a.home_team_college_id, a.away_team_college_id, b.home_team_roster_ref, b.away_team_roster_ref, a.season, a.event_id FROM events a LEFT JOIN event_details b on a.event_id = b.event_id
                WHERE a.season BETWEEN %s AND %s""",(start, end),)
        results = cursor.fetchall()
        # make results a list of dicts
        for result in results:
            results_list.append(
                {
                    "home_team": result[0],
                    "away_team": result[1],
                    "home_team_roster_url": result[2],
                    "away_team_roster_url": result[3],
                    "season": result[4],
                    "event_id": result[5],
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_player_stats_ref(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    results_list = []
    try:
        cursor.execute("""SELECT team_id, athlete_id, event_id, season, stats_ref FROM event_rosters where did_not_play = 0 and season BETWEEN %s and %s""",
            (start, end),)
        results = cursor.fetchall()
        # make results a list of dicts
        for result in results:
            results_list.append(
                {
                    "team_id": result[0],
                    "athlete_id": result[1],
                    "event_id": result[2],
                    "season": result[3],
                    "stats_ref": result[4],
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_event_odds(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    results_list = []
    try:
        cursor.execute("""SELECT a.event_id, b.odds_ref, a.home_team_college_id, a.away_team_college_id, a.season FROM events a LEFT JOIN event_details b on a.event_id = b.event_id
                WHERE b.odds_ref is not null and a.season BETWEEN %s AND %s""",(start, end),)
        results = cursor.fetchall()
        # make results a list of dicts
        for result in results:
            results_list.append(
                {
                    "event_id": result[0],
                    "odds_ref": result[1],
                    "home_team": result[2],
                    "away_team": result[3],
                    "season": result[4],
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()



def check_if_team_exists(team_id, season):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT espn_id FROM teams WHERE espn_id = %s AND season = %s",
            (team_id, season),
        )
        result = cursor.fetchone()
        if result is None:
            return False
        else:
            return True
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def get_event_date_range():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT MIN(date), MAX(date) FROM events"
        )
        result = cursor.fetchone()
        min_date = result[0]
        max_date = result[1]
        return min_date, max_date
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_team_ats_link_and_record_link_from_db(start, end):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT espn_id, season, ats_link, record_link FROM teams WHERE (season BETWEEN %s AND %s) AND ats_link IS NOT NULL AND record_link IS NOT NULL",
            (start, end),
        )
        results = cursor.fetchall()
        # make results a list of dicts
        results_list = []
        for result in results:
            results_list.append(
                {
                    "team_id": result[0],
                    "season": result[1],
                    "ats_link": result[2],
                    "record_link": result[3],
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_event_odds_details_links():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT event_id, provider_id, home_team_id, away_team_id, spread_details_ref, total_details_ref, moneyline_details_ref FROM event_odds WHERE spread_details_ref IS NOT NULL OR total_details_ref IS NOT NULL OR moneyline_details_ref IS NOT NULL"
        )
        results = cursor.fetchall()
        # make results a list of dicts
        results_list = []
        for result in results:
            results_list.append(
                {
                    "event_id": result[0],
                    "provider_id": result[1],
                    "home_team_id": result[2],
                    "away_team_id": result[3],
                    "spread_details_ref": result[4],
                    "total_details_ref": result[5],
                    "moneyline_details_ref": result[6],
                    
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()



def get_event_odds_lines_links():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT s.event_id, s.provider_id, s.home_team_id, s.away_team_id, s.movement_ref as spread_movement_ref, 
            m.movement_ref as moneyline_movement_ref, t.movement_ref as total_movement_ref FROM spread_odds_details s
            LEFT JOIN moneyline_odds_details m on s.event_id = m.event_id and s.provider_id = m.provider_id
            LEFT JOIN total_odds_details t on s.event_id = t.event_id and s.provider_id = t.provider_id
            WHERE s.movement_ref IS NOT NULL OR m.movement_ref IS NOT NULL OR t.movement_ref IS NOT NULL"""
        )
        results = cursor.fetchall()
        # make results a list of dicts
        results_list = []
        for result in results:
            results_list.append(
                {
                    "event_id": result[0],
                    "provider_id": result[1],
                    "home_team_id": result[2],
                    "away_team_id": result[3],
                    "spread_movement_ref": result[4],
                    "moneyline_movement_ref": result[5],
                    "total_movement_ref": result[6],
                    
                }
            )
        return results_list
    except MySQLdb.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()
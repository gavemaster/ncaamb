# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TeamItem(scrapy.Item):
    team_name = scrapy.Field()
    college_espn_id = scrapy.Field()
    team_location = scrapy.Field()
    team_conference = scrapy.Field()
    team_abbr = scrapy.Field()
    season = scrapy.Field()
    team_events_ref = scrapy.Field()


class ConferenceItem(scrapy.Item):
    name = scrapy.Field()
    conference_id = scrapy.Field()


class CollegeItem(scrapy.Item):
    team_name = scrapy.Field()
    college_espn_id = scrapy.Field()
    team_location = scrapy.Field()


class TeamOutcomesItem(scrapy.Item):
    college_espn_id = scrapy.Field()
    season = scrapy.Field()
    ats_overall_wins = scrapy.Field(default=0)
    ats_overall_losses = scrapy.Field(default=0)
    ats_overall_pushes = scrapy.Field(default=0)
    ats_favorite_wins = scrapy.Field(default=0)
    ats_favorite_losses = scrapy.Field(default=0)
    ats_favorite_pushes = scrapy.Field(default=0)
    ats_underdog_wins = scrapy.Field(default=0)
    ats_underdog_losses = scrapy.Field(default=0)
    ats_underdog_pushes = scrapy.Field(default=0)
    ats_away_wins = scrapy.Field(default=0)
    ats_away_losses = scrapy.Field(default=0)
    ats_away_pushes = scrapy.Field(default=0)
    ats_home_wins = scrapy.Field(default=0)
    ats_home_losses = scrapy.Field(default=0)
    ats_home_pushes = scrapy.Field(default=0)
    ats_away_favorite_wins = scrapy.Field(default=0)
    ats_away_favorite_losses = scrapy.Field(default=0)
    ats_away_favorite_pushes = scrapy.Field(default=0)
    ats_away_underdog_wins = scrapy.Field(default=0)
    ats_away_underdog_losses = scrapy.Field(default=0)
    ats_away_underdog_pushes = scrapy.Field(default=0)
    ats_home_favorite_wins = scrapy.Field(default=0)
    ats_home_favorite_losses = scrapy.Field(default=0)
    ats_home_favorite_pushes = scrapy.Field(default=0)
    ats_home_underdog_wins = scrapy.Field(default=0)
    ats_home_underdog_losses = scrapy.Field(default=0)
    ats_home_underdog_pushes = scrapy.Field(default=0)
    team_ovr_wins = scrapy.Field(default=0)
    team_ovr_losses = scrapy.Field(default=0)
    team_ovr_ot_losses = scrapy.Field(default=0)
    team_ovr_ot_wins = scrapy.Field(default=0)
    team_ovr_point_diff = scrapy.Field(default=0)
    team_ovr_avg_points = scrapy.Field(default=0)
    team_ovr_avg_points_allowed = scrapy.Field(default=0)
    team_ovr_division_win_percentage = scrapy.Field(default=0)
    team_ovr_games_played = scrapy.Field(default=0)
    team_home_wins = scrapy.Field(default=0)
    team_home_losses = scrapy.Field(default=0)
    team_home_ot_losses = scrapy.Field(default=0)
    team_home_ot_wins = scrapy.Field(default=0)
    team_home_point_diff = scrapy.Field(default=0)
    team_home_avg_points = scrapy.Field(default=0)
    team_home_avg_points_allowed = scrapy.Field(default=0)
    team_home_division_win_percentage = scrapy.Field(default=0)
    team_home_games_played = scrapy.Field(default=0)
    team_away_wins = scrapy.Field(default=0)
    team_away_losses = scrapy.Field(default=0)
    team_away_ot_losses = scrapy.Field(default=0)
    team_away_ot_wins = scrapy.Field(default=0)
    team_away_point_diff = scrapy.Field(default=0)
    team_away_avg_points = scrapy.Field(default=0)
    team_away_avg_points_allowed = scrapy.Field(default=0)
    team_away_division_win_percentage = scrapy.Field(default=0)
    team_away_games_played = scrapy.Field(default=0)
    team_versus_ap_top_25_wins = scrapy.Field(default=0)
    team_versus_ap_top_25_losses = scrapy.Field(default=0)
    team_versus_ap_top_25_ot_losses = scrapy.Field(default=0)
    team_versus_ap_top_25_ot_wins = scrapy.Field(default=0)
    team_versus_ap_top_25_point_diff = scrapy.Field(default=0)
    team_versus_ap_top_25_avg_points = scrapy.Field(default=0)
    team_versus_ap_top_25_avg_points_allowed = scrapy.Field(default=0)
    team_versus_ap_top_25_division_win_percentage = scrapy.Field(default=0)
    team_versus_ap_top_25_games_played = scrapy.Field(default=0)
    team_versus_conf_wins = scrapy.Field(default=0)
    team_versus_conf_losses = scrapy.Field(default=0)
    team_versus_conf_ot_losses = scrapy.Field(default=0)
    team_versus_conf_ot_wins = scrapy.Field(default=0)
    team_versus_conf_point_diff = scrapy.Field(default=0)
    team_versus_conf_avg_points = scrapy.Field(default=0)
    team_versus_conf_avg_points_allowed = scrapy.Field(default=0)
    team_versus_conf_division_win_percentage = scrapy.Field(default=0)
    team_versus_conf_games_played = scrapy.Field(default=0)
    last_updated = scrapy.Field()


class ConferenceDetailsItem(scrapy.Item):
    name = scrapy.Field()
    short_name = scrapy.Field()
    conference_id = scrapy.Field()
    conference_standings_ref = scrapy.Field()
    conference_teams_ref = scrapy.Field()
    season = scrapy.Field()


class EventItem(scrapy.Item):
    name = scrapy.Field()
    shortname = scrapy.Field()
    event_date = scrapy.Field()
    event_time = scrapy.Field()
    venue_id = scrapy.Field()
    home_team_college_id = scrapy.Field()
    away_team_college_id = scrapy.Field()
    home_team_score = scrapy.Field()
    away_team_score = scrapy.Field()
    event_id = scrapy.Field()
    season = scrapy.Field()
    week = scrapy.Field()
    season_type = scrapy.Field()


class EventDetailsItem(scrapy.Item):
    event_id = scrapy.Field()
    odds_ref = scrapy.Field()
    neutral_site = scrapy.Field()
    division_game = scrapy.Field()
    conference_game = scrapy.Field()
    home_team_stats_ref = scrapy.Field()
    away_team_stats_ref = scrapy.Field()
    home_team_roster_ref = scrapy.Field()
    away_team_roster_ref = scrapy.Field()
    event_details_ref = scrapy.Field()
    event_ref = scrapy.Field()
    status = scrapy.Field()
    status_ref = scrapy.Field()
    home_team_rank = scrapy.Field()
    away_team_rank = scrapy.Field()
    home_team_win = scrapy.Field()
    away_team_win = scrapy.Field()
    home_team_record_ref = scrapy.Field()
    away_team_record_ref = scrapy.Field()


class VenueItem(scrapy.Item):
    name = scrapy.Field()
    venue_id = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    capacity = scrapy.Field()
    indoor = scrapy.Field()
    venue_ref = scrapy.Field()


class AthleteItem(scrapy.Item):
    athlete_ref = scrapy.Field()
    id = scrapy.Field()
    name = scrapy.Field()
    first_name = scrapy.Field()
    last_name = scrapy.Field()
    birthplace = scrapy.Field()


class AthleteDetailsItem(scrapy.Item):
    athlete_id = scrapy.Field()
    season = scrapy.Field()
    team_id = scrapy.Field()
    jersey_number = scrapy.Field()
    position = scrapy.Field()
    exp = scrapy.Field()
    class_year = scrapy.Field()
    height = scrapy.Field()
    weight = scrapy.Field()
    status = scrapy.Field()


class RosterItem(scrapy.Item):
    athlete_id = scrapy.Field()
    team_id = scrapy.Field()
    season = scrapy.Field()
    starter = scrapy.Field()
    did_not_play = scrapy.Field()
    ejected = scrapy.Field()
    event_id = scrapy.Field()
    stats_ref = scrapy.Field()
    athlete_ref = scrapy.Field()
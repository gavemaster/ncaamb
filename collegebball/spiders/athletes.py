import scrapy
import json
import collegebball.database as db
from collegebball.items import AthleteItem, AthleteDetailsItem


class AthletesSpider(scrapy.Spider):
    name = "athletes"
    start_urls = []  # This will be populated from the script
    athlete_ids = set()

    def start_requests(self):
        id_list = db.get_athlete_ids_from_db()
        self.athlete_ids = set(id_list)
        for url in self.start_urls:
            yield scrapy.Request(
                url=url["athlete_ref"],
                callback=self.parse_athlete,
                meta={
                    "season": url["season"],
                },
            )

    def parse_athlete(self, response):
        data = json.loads(response.body)
        athlete = AthleteItem()
        athlete_id = data["id"]
        if athlete_id not in self.athlete_ids:
            athlete["athlete_ref"] = data["$ref"]
            athlete["id"] = athlete_id
            athlete["name"] = data["displayName"]

            if "firstName" in data:
                athlete["first_name"] = data["firstName"]
            else:
                athlete["first_name"] = None

            if "lastName" in data:
                athlete["last_name"] = data["lastName"]
            else:
                athlete["last_name"] = None

            if "birthplace" in data:
                if "city" in data["birthplace"]:
                    city = data["birthplace"]["city"]
                else:
                    city = None

                if "state" in data["birthplace"]:
                    state = data["birthplace"]["state"]
                else:
                    state = None

                if "country" in data["birthplace"]:
                    country = data["birthplace"]["country"]
                else:
                    country = None

                athlete["birthplace"] = f"{city} ,  {state} ,  {country}"
            else:
                athlete["birthplace"] = None

            yield athlete

            self.athlete_ids.add(athlete_id)

        player = AthleteDetailsItem()
        player["athlete_id"] = athlete_id
        player["season"] = response.meta["season"]

        if "team" in data:
            team_url = data["team"]["$ref"]
            parts = team_url.split("/teams/")
            team_id_part = parts[1].split("?")[0]
            player["team_id"] = int(team_id_part)
        else:
            player["team_id"] = None

        if "jersey" in data:
            player["jersey_number"] = data["jersey"]
        else:
            player["jersey_number"] = None

        if "position" in data:
            player["position"] = data["position"]["name"]
        else:
            player["position"] = None

        if "experience" in data:
            player["exp"] = data["experience"]["years"]
            player["class_year"] = data["experience"]["displayValue"]
        else:
            player["exp"] = None
            player["class_year"] = None

        if "status" in data:
            player["status"] = data["status"]["name"]
        else:
            player["status"] = None

        if "height" in data:
            player["height"] = data["height"]
        else:
            player["height"] = None

        if "weight" in data:
            player["weight"] = data["weight"]
        else:
            player["weight"] = None

        yield player

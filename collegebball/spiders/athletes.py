import scrapy
import json
import collegebball.database as db
from collegebball.items import AthleteItem
import collegebball.utils as utils

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
                callback=self.parse_athlete
            )

    def parse_athlete(self, response):
        data = json.loads(response.body)
        athlete = AthleteItem()
        athlete_id = data["id"]

        
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


        if "headshot" in data:
            athlete["headshot"] = data["headshot"]["href"]
        else:
            athlete["headshot"] = None
        yield athlete

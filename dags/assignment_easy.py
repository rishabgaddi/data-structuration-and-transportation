# names: Rishab Gaddi and John Kuloth Peterson Stephen Arumugam
from airflow.decorators import dag, task
from datetime import datetime
import json
from utils import read_from_api, json_dump_to_file, to_seconds_since_epoch

@dag(
  start_date=datetime(2023, 1, 24),
  schedule_interval="0 1 * * *",
)
def assignment_easy():

  BASE_URL = "https://opensky-network.org/api"

  @task(multiple_outputs=True)
  def read_data() -> dict:
    params = {
        "airport": "LFPG", # ICAO code for CDG
        "begin": to_seconds_since_epoch("2022-12-01"),
        "end": to_seconds_since_epoch("2022-12-02")
    }
    flights = read_from_api(f"{BASE_URL}/flights/departure", params)
    print(json.dumps(flights))
    return {"flights": flights}

  @task
  def write_data(flights: dict) -> None:
    # write the flights into a json file
    json_dump_to_file(flights["flights"], "./dags/flights_easy.json")

  flights = read_data()
  write_data(flights)

_ = assignment_easy()

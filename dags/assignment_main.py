# names: Rishab Gaddi and John Kuloth Peterson Stephen Arumugam
from airflow.decorators import dag, task
from datetime import datetime
import json
from utils import read_from_api, json_dump_to_file, to_seconds_since_epoch

@dag(
  schedule=None,
  start_date=datetime(2023, 1, 24),
  catchup=False
)
def assignment_main():

  BASE_URL = "https://opensky-network.org/api"

  @task
  def read_data() -> str:
    params = {
        "airport": "LFPG", # ICAO code for CDG
        "begin": to_seconds_since_epoch("2022-12-01"),
        "end": to_seconds_since_epoch("2022-12-02")
    }
    flights = json.dumps(read_from_api(f"{BASE_URL}/flights/departure", params))
    print(flights)
    return flights

  @task
  def write_data(flights: str) -> None:
    # write the flights into a json file
    json_dump_to_file(json.loads(flights), "./dags/flights_main.json")

  flights = read_data()
  write_data(flights)

_ = assignment_main()

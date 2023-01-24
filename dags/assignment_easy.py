# name: Rishab Gaddi, John Kuloth Peterson Stephen Arumugam
from airflow.decorators import dag, task
from datetime import datetime, date
from time import mktime
import requests
import json

@dag(
  start_date=datetime(2023, 1, 24),
  schedule_interval="0 1 * * *",
)
def assignment_easy():

  BASE_URL = "https://opensky-network.org/api"

  def to_seconds_since_epoch(input_date: str) -> int:
    return int(mktime(date.fromisoformat(input_date).timetuple()))

  @task(multiple_outputs=True)
  def read_data() -> dict:
    params = {
        "airport": "LFPG", # ICAO code for CDG
        "begin": to_seconds_since_epoch("2022-12-01"),
        "end": to_seconds_since_epoch("2022-12-02")
    }
    cdg_flights = f"{BASE_URL}/flights/departure"
    response = requests.get(cdg_flights, params=params)
    flights = response.json()
    print(json.dumps(flights))
    return {"flights": flights}

  @task
  def write_data(flights: dict) -> None:
    # write the flights into a json file
    with open("./dags/flights_easy.json", "w") as f:
      json.dump(flights["flights"], f)

  flights = read_data()
  write_data(flights)

_ = assignment_easy()

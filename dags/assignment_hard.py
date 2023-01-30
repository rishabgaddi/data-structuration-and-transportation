# names: Rishab Gaddi and John Kuloth Peterson Stephen Arumugam
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dataclasses import astuple, dataclass
from utils import read_from_api, json_dump_to_file, to_seconds_since_epoch
import json
import sqlite3

@dataclass
class Flight:
  icao24: str
  firstSeen: int
  estDepartureAirport: str
  lastSeen: int
  estArrivalAirport: str
  callsign: str
  estDepartureAirportHorizDistance: int
  estDepartureAirportVertDistance: int
  estArrivalAirportHorizDistance: int
  estArrivalAirportVertDistance: int
  departureAirportCandidatesCount: int
  arrivalAirportCandidatesCount: int

@dag(
  schedule=None,
  start_date=datetime(2023, 1, 29),
  catchup=False
)
def assignment_hard():

  BASE_URL = "https://opensky-network.org/api"

  @task(multiple_outputs=True)
  def read_data(ds=None) -> dict:
    lastWeek = datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=7)
    lastWeekPlusOne = lastWeek + timedelta(days=1)

    params = {
        "airport": "LFPG", # ICAO code for CDG
        "begin": to_seconds_since_epoch(lastWeek.strftime("%Y-%m-%d")),
        "end": to_seconds_since_epoch(lastWeekPlusOne.strftime("%Y-%m-%d"))
    }
    flights = read_from_api(f"{BASE_URL}/flights/departure", params)
    print(json.dumps(flights))
    return {"flights": flights}

  @task
  def write_data(flights: dict) -> None:
    # write the flights into a json file
    json_dump_to_file(flights["flights"], "./dags/flights_hard.json")

  @task
  def write_to_db(flights: dict) -> None:
    flights = [astuple(Flight(**row)) for row in flights["flights"]]
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE flights (icao24 text, firstSeen int, estDepartureAirport text, lastSeen int, estArrivalAirport text, callsign text, estDepartureAirportHorizDistance int, estDepartureAirportVertDistance int, estArrivalAirportHorizDistance int, estArrivalAirportVertDistance int, departureAirportCandidatesCount int, arrivalAirportCandidatesCount int)")
    connection.executemany("INSERT INTO flights VALUES(?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?)", flights)
    connection.commit()
    res = connection.execute("SELECT * FROM flights")
    for row in res:
      print(row)

  flights = read_data()
  write_data(flights)
  write_to_db(flights)

_ = assignment_hard()

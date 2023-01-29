# names: Rishab Gaddi and John Kuloth Peterson Stephen Arumugam
from airflow.decorators import dag, task
from datetime import datetime
import json
from utils import read_from_api, json_dump_to_file, to_seconds_since_epoch

@dag(
  schedule=None,
  start_date=datetime(2023, 1, 29),
  catchup=False
)
def assignment_medium():

  BASE_URL = "https://www.alphavantage.co/query"

  @task(multiple_outputs=True)
  def read_data() -> dict:
    params = {
        "function": "SYMBOL_SEARCH",
        "keywords": "tencent",
        "apikey": "demo"
    }
    search_results = read_from_api(f"{BASE_URL}", params)
    print(json.dumps(search_results))
    return search_results

  @task
  def transformation(data: dict) -> None:
    search_list = data["bestMatches"]
    # Count number of times Frankfurt region appeared in the search results
    count = 0
    for match in search_list:
      if match["4. region"] == "Frankfurt":
        count += 1
    print("Number of times Frankfurt region appeared in the search results: ", count)

  @task
  def write_data(data: dict) -> None:
    json_dump_to_file(data, "./dags/search_results.json")

  results = read_data()
  print(results)
  transformation(results)
  write_data(results)

_ = assignment_medium()

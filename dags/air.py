import requests

url = "https://www.searchapi.io/api/v1/search"
params = {
  "engine": "google_flights_calendar",
  "flight_type": "round_trip",
  "departure_id": "ANC",
  "arrival_id": "LHR",
  "outbound_date": "2025-12-11",
  "return_date": "2025-12-12",
  "outbound_date_start": "2025-12-11",
  "outbound_date_end": "2025-12-13",
  "return_date_start": "2025-12-11",
  "return_date_end": "2025-12-14"
}

response = requests.get(url, params=params)
print(response.text)
"""Gathering weather data for every game from the Open-Meteo API."""
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# %%
# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)
# %%
"""Thanks to perplexcity for the data"""
data = [
    {"Team": "ARI", "City": "Glendale", "Closed": 1, "Lat": 33.527765, "Lon": -112.262360},
    {"Team": "ATL", "City": "Atlanta", "Closed": 1, "Lat": 33.755420, "Lon": -84.401130},
    {"Team": "BAL", "City": "Baltimore", "Closed": 0, "Lat": 39.277969, "Lon": -76.622767},
    {"Team": "BUF", "City": "Orchard Park", "Closed": 0, "Lat": 42.773683, "Lon": -78.786789},
    {"Team": "CAR", "City": "Charlotte", "Closed": 0, "Lat": 35.225833, "Lon": -80.852778},
    {"Team": "CHI", "City": "Chicago", "Closed": 0, "Lat": 41.862306, "Lon": -87.616672},
    {"Team": "CIN", "City": "Cincinnati", "Closed": 0, "Lat": 39.095444, "Lon": -84.516039},
    {"Team": "CLE", "City": "Cleveland", "Closed": 0, "Lat": 41.506052, "Lon": -81.699564},
    {"Team": "DAL", "City": "Arlington", "Closed": 1, "Lat": 32.747778, "Lon": -97.092778},
    {"Team": "DEN", "City": "Denver", "Closed": 0, "Lat": 39.743936, "Lon": -105.020097},
    {"Team": "DET", "City": "Detroit", "Closed": 1, "Lat": 42.340021, "Lon": -83.045777},
    {"Team": "GB", "City": "Green Bay", "Closed": 0, "Lat": 44.501308, "Lon": -88.062317},
    {"Team": "HOU", "City": "Houston", "Closed": 1, "Lat": 29.684722, "Lon": -95.410707},
    {"Team": "IND", "City": "Indianapolis", "Closed": 1, "Lat": 39.759991, "Lon": -86.163712},
    {"Team": "JAX", "City": "Jacksonville", "Closed": 0, "Lat": 30.323925, "Lon": -81.637356},
    {"Team": "KC", "City": "Kansas City", "Closed": 0, "Lat": 39.048935, "Lon": -94.484039},
    {"Team": "LAC", "City": "Inglewood", "Closed": 1, "Lat": 33.953587, "Lon": -118.339630},
    {"Team": "LAR", "City": "Inglewood", "Closed": 1, "Lat": 33.953587, "Lon": -118.339630},
    {"Team": "LV", "City": "Las Vegas", "Closed": 1, "Lat": 36.090794, "Lon": -115.183952},
    {"Team": "MIA", "City": "Miami Gardens", "Closed": 0, "Lat": 25.958056, "Lon": -80.238889},
    {"Team": "MIN", "City": "Minneapolis", "Closed": 1, "Lat": 44.973889, "Lon": -93.258056},
    {"Team": "NE", "City": "Foxborough", "Closed": 0, "Lat": 42.090944, "Lon": -71.264344},
    {"Team": "NO", "City": "New Orleans", "Closed": 1, "Lat": 29.951061, "Lon": -90.081267},
    {"Team": "NYG", "City": "East Rutherford", "Closed": 0, "Lat": 40.813611, "Lon": -74.074444},
    {"Team": "NYJ", "City": "East Rutherford", "Closed": 0, "Lat": 40.813611, "Lon": -74.074444},
    {"Team": "PHI", "City": "Philadelphia", "Closed": 0, "Lat": 39.900833, "Lon": -75.167500},
    {"Team": "PIT", "City": "Pittsburgh", "Closed": 0, "Lat": 40.446667, "Lon": -80.015833},
    {"Team": "SEA", "City": "Seattle", "Closed": 0, "Lat": 47.595097, "Lon": -122.332245},
    {"Team": "SF", "City": "Santa Clara", "Closed": 0, "Lat": 37.403000, "Lon": -121.970000},
    {"Team": "TB", "City": "Tampa", "Closed": 0, "Lat": 27.975833, "Lon": -82.503333},
    {"Team": "TEN", "City": "Nashville", "Closed": 0, "Lat": 36.166389, "Lon": -86.771389},
    {"Team": "WAS", "City": "Landover", "Closed": 0, "Lat": 38.907778, "Lon": -76.864444}
]

# %%
weather_df = pd.DataFrame()
# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
for city in data:
    params = {
        "latitude": city["Lat"],
        "longitude": city["Lon"],
        "start_date": "2022-09-01",
        "end_date": "2022-12-31",
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                  "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean",
                  "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max",
                  "wind_gusts_10m_max", "wind_direction_10m_dominant"]
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]


    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_apparent_temperature_max = daily.Variables(4).ValuesAsNumpy()
    daily_apparent_temperature_min = daily.Variables(5).ValuesAsNumpy()
    daily_apparent_temperature_mean = daily.Variables(6).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(7).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(8).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(9).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(10).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(11).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(12).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(13).ValuesAsNumpy()

    daily_data = {"gameDate": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
    daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
    daily_data["apparent_temperature_mean"] = daily_apparent_temperature_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["rain_sum"] = daily_rain_sum
    daily_data["snowfall_sum"] = daily_snowfall_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    daily_dataframe = pd.DataFrame(data=daily_data)
    daily_dataframe['City'] = city["City"]
    daily_dataframe['Team'] = city["Team"]

    weather_df = pd.concat([weather_df, daily_dataframe])
    # print(daily_dataframe.head(5))

# %%
weather_df.gameDate = pd.to_datetime(weather_df.gameDate).dt.date
# %%

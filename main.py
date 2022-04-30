import requests
import csv
from pprint import pprint
from time import strftime
from datetime import date
import pandas as pd

CURRENT_YEAR = date.today().year
CURRENT_MONTH = date.today().month
CURRENT_DAY = date.today().day


def get_locations(csv_file):
    # I read the exists data from the weather_data file. If there is a new location with new date that is not
    # exists in the locations file, I add it to a locations list (with all relevant params) for updating tables.
    # I assume that we generate the data for each date, so for new locations we will start generate data for
    # the first time that they appear in the locations file.
    # For exists locations, we will generate the data for the current date
    locations_df = pd.read_csv('weather_data.csv')
    exists_locations = locations_df[['metaweather_id', 'applicable_date']].values.tolist()
    locations_dict = {}
    with open(csv_file, newline='') as inputfile:
        reader = csv.reader(inputfile)
        next(reader, None)
        for row in reader:
            location_for_checking = []
            location_for_checking.append(int(row[3]))
            location_for_checking.append(date.today().strftime("%Y-%m-%d"))
            if location_for_checking not in exists_locations:
                params_location_dict = {}
                params_location_dict["country"] = row[1]
                params_location_dict["location"] = row[2]
                params_location_dict["metaweather_id"] = row[3]
                locations_dict[row[3]] = params_location_dict

    return locations_dict


def get_data_from_api(locations_dict):
    # for every relevant location, I send API request
    for location_id, params in locations_dict.items():
        url = f'http://www.metaweather.com/api/location/{location_id}/{CURRENT_YEAR}/{CURRENT_MONTH}/{CURRENT_DAY}'
        r = requests.get(url)
        response = r.json()
        create_meta_data_for_location(location_id, locations_dict, response)
    return locations_dict


def create_meta_data_for_location(location_id, locations_dict, response):
    # For every location, I create lists of max and min temps, and find the max temp from max temps list and min temp
    # from min temp list. I add it to the params list, and also timestamp
    max_temps_list = []
    min_temps_list = []
    for dict in response:
        max_temps_list.append(dict["max_temp"])
        min_temps_list.append(dict["min_temp"])
    locations_dict[location_id]["daily_max_temp"] = round(max(max_temps_list), 1)
    locations_dict[location_id]["daily_min_temp"] = round(min(min_temps_list), 1)
    locations_dict[location_id]["applicable_date"] = date.today().strftime("%Y-%m-%d")


def create_weather_data_tbl(locations_dict):
    # I added the new data to the exists data in the csv file
    weather_data_df = pd.DataFrame.from_dict(locations_dict.values())
    weather_data_df['timestamp'] = strftime("%Y-%m-%d %H:%M:%S")
    weather_data_df.to_csv('weather_data.csv', index=None, mode='a', header=False)


def create_country_agg_tbl():
    # For each time we update the weather_data file, we need to calculate the new aggregations fields in the
    # country_agg file. I read all the data from weather_data and calculate the new aggregations.
    weather_data_df = pd.read_csv('weather_data.csv')
    country_agg_df = weather_data_df.groupby(['country', 'applicable_date']).apply(temps_agg)
    country_agg_df['timestamp'] = strftime("%Y-%m-%d %H:%M:%S")
    country_agg_df.to_csv('country_agg.csv', index=['country', 'applicable_date'],  header=True)


def temps_agg(df):
    names = {
        'daily_avg_min_temp': df['daily_min_temp'].mean(),
        'daily_min_temp':  df['daily_min_temp'].min(),
        'daily_avg_max_temp': df['daily_max_temp'].mean(),
        'daily_max_temp': df['daily_max_temp'].max()}

    return pd.Series(names, index=['daily_avg_min_temp', 'daily_min_temp', 'daily_avg_max_temp', 'daily_max_temp'])


if __name__ == "__main__":
    locations = get_locations("locations.csv")
    if locations:
        # If there are no new locations and dates, we dont need to update any file.
        locations_dict = get_data_from_api(locations)
        create_weather_data_tbl(locations_dict)
        create_country_agg_tbl()

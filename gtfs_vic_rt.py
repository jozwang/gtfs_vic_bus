import requests
import streamlit as st
import pandas as pd
from google.transit import gtfs_realtime_pb2
import datetime

# Convert timestamp
def convert_unix_to_time(unix_timestamp):
    try:
        return datetime.datetime.fromtimestamp(unix_timestamp).strftime('%H:%M:%S')
    except:
        return "N/A"

# Parse route/direction from trip_id
def parse_trip_id(trip_id):
    try:
        parts = trip_id.split('-')
        route = parts[1] if len(parts) > 1 else "Unknown"
        direction = trip_id.split('--')[1].split('-')[0] if '--' in trip_id else "Unknown"
        return route, direction
    except:
        return "Unknown", "Unknown"

# Streamlit setup
st.set_page_config(page_title="Metro Bus Snapshot", layout="wide")
st.title("🚍 Metro Bus Realtime Snapshot – VIC")

# API setup
api_key = "321077bd7df146b891bde8960ffa1893"
url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates"
headers = {"Ocp-Apim-Subscription-Key": api_key}
params = {"subscription-key": api_key}

# Request feed
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            trip = trip_update.trip
            vehicle = trip_update.vehicle
            route, direction = parse_trip_id(trip.trip_id)

            for stop in trip_update.stop_time_update:
                stop_sequence = stop.stop_sequence if stop.HasField("stop_sequence") else "N/A"
                arrival_time = stop.arrival.time if stop.HasField("arrival") and stop.arrival.HasField("time") else None
                departure_time = stop.departure.time if stop.HasField("departure") and stop.departure.HasField("time") else None
                arrival_delay = stop.arrival.delay if stop.HasField("arrival") and stop.arrival.HasField("delay") else "N/A"

                records.append({
                    "Vehicle ID": vehicle.id if trip_update.HasField("vehicle") and vehicle.id else "N/A",
                    "Trip ID": trip.trip_id,
                    "Start Date": trip.start_date,
                    "Start Time": trip.start_time,
                    "Route": route,
                    "Direction": direction,
                    "Stop Sequence": stop_sequence,
                    "Stop Arrival Delay": arrival_delay

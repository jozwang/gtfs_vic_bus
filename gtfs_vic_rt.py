import requests
import streamlit as st
import pandas as pd
from google.transit import gtfs_realtime_pb2

# App config
st.set_page_config(page_title="Metro Bus Snapshot", layout="wide")
st.title("🚍 Metro Bus Realtime Snapshot – VIC")

# API endpoint and key
api_key = "321077bd7df146b891bde8960ffa1893"
base_url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates"

# Combine header and query for security schemes
headers = {
    "Ocp-Apim-Subscription-Key": api_key
}
params = {
    "subscription-key": api_key
}

# Make authenticated request
response = requests.get(base_url, headers=headers, params=params)

if response.status_code == 200:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records = []
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            vehicle = entity.trip_update.vehicle
            route= trip.route
             
            records.append({
                "Vehicle ID": vehicle.id if vehicle and vehicle.id else "N/A",
                "Route ID": trip.route if route and route.id else "n/a",
                "Trip ID": trip.trip_id
            })

    df = pd.DataFrame(records)

    if not df.empty:
        st.dataframe(df)
    else:
        st.warning("No vehicle or trip updates currently available.")
else:
    st.error(f"API call failed. Status code: {response.status_code}")

import requests
import streamlit as st
import pandas as pd
from google.transit import gtfs_realtime_pb2
import datetime

# --- Utility Functions ---

def convert_unix_to_time(unix_timestamp):
    """Converts a Unix timestamp to HH:MM:SS format. Returns 'N/A' if invalid."""
    if unix_timestamp is None or not isinstance(unix_timestamp, (int, float)):
        return "N/A"
    try:
        return datetime.datetime.fromtimestamp(unix_timestamp).strftime('%H:%M:%S')
    except (ValueError, TypeError):
        return "N/A"

def parse_trip_id(trip_id):
    """Extracts Route and Direction from a PTV GTFS Realtime trip_id."""
    route, direction = "Unknown", "Unknown"
    try:
        parts = trip_id.split('-')
        if len(parts) > 1:
            route = parts[1]

        # More robust direction parsing
        direction_match = trip_id.split('--')
        if len(direction_match) > 1:
            direction_sub_parts = direction_match[1].split('-')
            if len(direction_sub_parts) > 0:
                direction = direction_sub_parts[0]
    except Exception:
        # Log the exception for debugging in a real application
        pass
    return route, direction

# --- Streamlit Application Setup ---

st.set_page_config(page_title="Metro Bus Realtime Snapshot", layout="wide")
st.title("🚍 Metro Bus Realtime Snapshot – VIC")

# --- API Configuration ---

api_key = "321077bd7df146b891bde8960ffa1893"
base_url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates"
headers = {"Ocp-Apim-Subscription-Key": api_key}
params = {"subscription-key": api_key}

# --- Data Fetching and Processing ---

@st.cache_data(ttl=30) # Cache data for 30 seconds to reduce API calls
def fetch_and_process_data():
    """Fetches data from the GTFS Realtime API and processes it into a DataFrame."""
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=10) # Added timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        records = []
        for entity in feed.entity:
            if entity.HasField("trip_update"):
                trip_update = entity.trip_update
                trip = trip_update.trip
                vehicle_id = trip_update.vehicle.id if trip_update.HasField("vehicle") and trip_update.vehicle.HasField("id") else "N/A"
                route, direction = parse_trip_id(trip.trip_id)

                for stop in trip_update.stop_time_update:
                    stop_sequence = stop.stop_sequence if stop.HasField("stop_sequence") else "N/A"
                    arrival_time_unix = stop.arrival.time if stop.HasField("arrival") and stop.arrival.HasField("time") else None
                    departure_time_unix = stop.departure.time if stop.HasField("departure") and stop.departure.HasField("time") else None
                    arrival_delay = stop.arrival.delay if stop.HasField("arrival") and stop.arrival.HasField("delay") else "N/A"

                    records.append({
                        "Vehicle ID": vehicle_id,
                        "Trip ID": trip.trip_id,
                        "Start Date": trip.start_date,
                        "Start Time": trip.start_time,
                        "Route": route,
                        "Direction": direction,
                        "Stop Sequence": stop_sequence,
                        "Stop Arrival Delay": arrival_delay,
                        "Arrival Time": convert_unix_to_time(arrival_time_unix),
                        "Departure Time": convert_unix_to_time(departure_time_unix)
                    })
        return pd.DataFrame(records)
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        st.error(f"An unexpected error occurred during data processing: {e}")
        return pd.DataFrame()

df = fetch_and_process_data()

# --- Streamlit App Logic ---

if not df.empty:
    st.write(f"Data last updated: {datetime.datetime.now().strftime('%H:%M:%S')}") # Show last update time

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Trips")

    # Get unique, sorted routes, ensuring 'Unknown' is handled
    all_routes = df["Route"].dropna().unique().tolist()
    if "Unknown" in all_routes:
        all_routes.remove("Unknown")
        all_routes = sorted(all_routes) + ["Unknown"]
    else:
        all_routes = sorted(all_routes)

    selected_route = st.sidebar.selectbox("Select Route", options=all_routes)

    # Filter directions based on selected route
    filtered_directions = df[df["Route"] == selected_route]["Direction"].dropna().unique().tolist()
    if "Unknown" in filtered_directions:
        filtered_directions.remove("Unknown")
        filtered_directions = sorted(filtered_directions) + ["Unknown"]
    else:
        filtered_directions = sorted(filtered_directions)

    selected_direction = st.sidebar.selectbox("Select Direction", options=filtered_directions)

    # Filter stop sequences based on selected route and direction
    filtered_stops = df[(df["Route"] == selected_route) & (df["Direction"] == selected_direction)]["Stop Sequence"].dropna().unique().tolist()
    # Convert to numeric for sorting if possible, then back to original type
    try:
        filtered_stops_numeric = sorted([s for s in filtered_stops if isinstance(s, (int, float))])
        filtered_stops_non_numeric = sorted([s for s in filtered_stops if not isinstance(s, (int, float))])
        filtered_stops = filtered_stops_numeric + filtered_stops_non_numeric
    except TypeError: # Fallback if mixed types cause issues with simple sort
        filtered_stops = sorted(filtered_stops)

    selected_stop_seq = st.sidebar.selectbox("Select Stop Sequence", options=filtered_stops)

    # --- Filtered Table ---
    st.subheader("🚏 Filtered Trip Data")

    filtered_df = df[
        (df["Route"] == selected_route) &
        (df["Direction"] == selected_direction) &
        (df["Stop Sequence"] == selected_stop_seq)
    ]

    if not filtered_df.empty:
        # Reset index for cleaner display in Streamlit
        st.dataframe(filtered_df.reset_index(drop=True), use_container_width=True)
    else:
        st.warning("No matching records found for the selected filters.")
else:
    st.info("No data available to display. Please check API connectivity or try again later.")

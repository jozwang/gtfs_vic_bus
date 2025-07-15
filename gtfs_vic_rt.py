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

    ## 1. Default route filter is 903
    all_routes = sorted(df["Route"].dropna().unique().tolist())
    # Ensure '903' is in options, handle 'Unknown'
    if "Unknown" in all_routes:
        all_routes.remove("Unknown")
    if '903' in all_routes:
        default_route_index = all_routes.index('903')
    else:
        default_route_index = 0 # Fallback to first item if 903 not found

    # Allow 'All' for all filters - add 'All' to the options list
    # For Route, 'All' is optional based on whether you always want a default route
    # If 'All' is desired for Route, uncomment the line below:
    # all_routes.insert(0, "All")
    # if '903' is not guaranteed, and you want 'All' to be default, set index to 0

    selected_route = st.sidebar.selectbox(
        "Select Route",
        options=all_routes,
        index=default_route_index # Set default to '903'
    )

    # Filter directions based on selected route (or all if 'All' route selected)
    if selected_route == "All":
        filtered_df_for_directions = df
    else:
        filtered_df_for_directions = df[df["Route"] == selected_route]

    ## 2. Allow "All" for all filters & 3. Default "All" for direction
    all_directions = sorted(filtered_df_for_directions["Direction"].dropna().unique().tolist())
    if "Unknown" in all_directions:
        all_directions.remove("Unknown")
    all_directions.insert(0, "All") # Add "All" option at the beginning
    
    # Set default to 'All'
    default_direction_index = all_directions.index("All") if "All" in all_directions else 0
    selected_direction = st.sidebar.selectbox(
        "Select Direction",
        options=all_directions,
        index=default_direction_index # Default to 'All'
    )

    # Filter stop sequences based on selected route and direction
    if selected_route == "All":
        filtered_df_for_stops = df
    else:
        filtered_df_for_stops = df[df["Route"] == selected_route]

    if selected_direction != "All":
        filtered_df_for_stops = filtered_df_for_stops[filtered_df_for_stops["Direction"] == selected_direction]

    ## 2. Allow "All" for all filters & 3. Default "All" for stop sequence
    all_stops = filtered_df_for_stops["Stop Sequence"].dropna().unique().tolist()
    
    # Convert to numeric for sorting if possible, handling "N/A" and other non-numeric
    numeric_stops = [s for s in all_stops if isinstance(s, (int, float)) and s != "N/A"]
    non_numeric_stops = [s for s in all_stops if not isinstance(s, (int, float)) or s == "N/A"]
    
    all_stops_sorted = sorted(numeric_stops) + sorted(non_numeric_stops) # Sort numeric then non-numeric
    all_stops_sorted.insert(0, "All") # Add "All" option at the beginning

    # Set default to 'All'
    default_stop_index = all_stops_sorted.index("All") if "All" in all_stops_sorted else 0
    selected_stop_seq = st.sidebar.selectbox(
        "Select Stop Sequence",
        options=all_stops_sorted,
        index=default_stop_index # Default to 'All'
    )

    # --- Apply Filters to DataFrame ---
    st.subheader("🚏 Filtered Trip Data")

    final_filtered_df = df.copy() # Start with a copy of the full DataFrame

    if selected_route != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Route"] == selected_route]
    
    if selected_direction != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Direction"] == selected_direction]
    
    if selected_stop_seq != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Stop Sequence"] == selected_stop_seq]


    if not final_filtered_df.empty:
        st.dataframe(final_filtered_df.reset_index(drop=True), use_container_width=True)
    else:
        st.warning("No matching records found for the selected filters.")
else:
    st.info("No data available to display. Please check API connectivity or try again later.")

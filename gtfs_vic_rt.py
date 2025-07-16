import requests
import streamlit as st
import pandas as pd
from google.transit import gtfs_realtime_pb2
import datetime
import zipfile
import io

# --- Utility Functions ---

def convert_unix_to_time(unix_timestamp):
    """
    Converts a Unix timestamp to HH:MM:SS format, adjusted for UTC+10.
    Returns 'N/A' if invalid.
    """
    if unix_timestamp is None or not isinstance(unix_timestamp, (int, float)):
        return "N/A"
    try:
        # Add 10 hours (10 * 3600 seconds) to the Unix timestamp for UTC+10
        adjusted_timestamp = unix_timestamp + (10 * 3600)
        return datetime.datetime.fromtimestamp(adjusted_timestamp, tz=datetime.timezone.utc).strftime('%H:%M:%S')
    except (ValueError, TypeError):
        return "N/A"

def time_string_to_seconds(time_str):
    """
    Converts a time string (HH:MM:SS) to total seconds from midnight.
    Handles times > 23:59:59 as per GTFS spec (e.g., 25:00:00).
    Returns None if invalid.
    """
    if not isinstance(time_str, str):
        return None
    try:
        parts = list(map(int, time_str.split(':')))
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return hours * 3600 + minutes * 60 + seconds
        return None
    except ValueError:
        return None

def parse_trip_id(trip_id):
    """Extracts Route and Direction from a PTV GTFS Realtime trip_id."""
    route, direction = "Unknown", "Unknown"
    if not isinstance(trip_id, str):
        return route, direction
    try:
        parts = trip_id.split('-')
        if len(parts) > 1:
            route = parts[1]

        direction_match = trip_id.split('--')
        if len(direction_match) > 1:
            direction_sub_parts = direction_match[1].split('-')
            if len(direction_sub_parts) > 0:
                direction = direction_sub_parts[0]
            else:
                direction = "Unknown"
    except Exception:
        pass # In a real app, you might log this error
    return route, direction

# --- Streamlit Application Setup ---

st.set_page_config(page_title="Metro Bus Realtime Snapshot", layout="wide")
st.title("🚍 Metro Bus Realtime Snapshot – VIC")

# --- API Configuration ---

api_key = "321077bd7df146b891bde8960ffa1893"
base_url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates"
headers = {"Ocp-Apim-Subscription-Key": api_key}
params = {"subscription-key": api_key}

# --- Static Data URLs ---
TRIPS_CSV_URL = "https://raw.githubusercontent.com/jozwang/gtfs_vic_bus/refs/heads/main/trips.csv"
ROUTES_CSV_URL = "https://raw.githubusercontent.com/jozwang/gtfs_vic_bus/refs/heads/main/routes.csv"
STOP_TIMES_ZIP_URL = "https://raw.githubusercontent.com/jozwang/gtfs_vic_bus/refs/heads/main/stop_times_4_kmel.zip" # New URL for stop_times.zip

# --- Data Loading Functions for Static GTFS ---

@st.cache_data(ttl=3600 * 24) # Cache for 24 hours as static data changes infrequently
def load_static_trips_data():
    """Loads trips.csv from GitHub and returns a dictionary mapping trip_id to route_id."""
    try:
        st.info(f"Attempting to load trips.csv from: {TRIPS_CSV_URL}")
        trips_df = pd.read_csv(TRIPS_CSV_URL)
        
        # Ensure trip_id is string for consistent merging/lookup
        trips_df['trip_id'] = trips_df['trip_id'].astype(str)
        
        if 'route_id' not in trips_df.columns:
            st.error("Error: 'route_id' column not found in the uploaded trips.csv.")
            return None
        
        # Create a dictionary for efficient lookup: trip_id -> route_id
        return trips_df.set_index('trip_id')['route_id'].to_dict()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching trips.csv from GitHub: {e}. Please check the URL or your internet connection.")
        return None
    except Exception as e:
        st.error(f"Error loading or processing trips.csv: {e}. Ensure it's a valid CSV with 'trip_id' and 'route_id' columns.")
        return None

@st.cache_data(ttl=3600 * 24) # Cache for 24 hours as static data changes infrequently
def load_static_routes_data():
    """Loads routes.csv from GitHub and returns a dictionary mapping route_id to route_short_name."""
    try:
        st.info(f"Attempting to load routes.csv from: {ROUTES_CSV_URL}")
        routes_df = pd.read_csv(ROUTES_CSV_URL)
        
        # Ensure route_id is string for consistent merging/lookup
        routes_df['route_id'] = routes_df['route_id'].astype(str)
        
        if 'route_id' not in routes_df.columns or 'route_short_name' not in routes_df.columns:
            st.error("Error: 'route_id' or 'route_short_name' column not found in the routes.csv.")
            return None
        
        # Create a dictionary for efficient lookup: route_id -> route_short_name
        return routes_df.set_index('route_id')['route_short_name'].to_dict()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching routes.csv from GitHub: {e}. Please check the URL or your internet connection.")
        return None
    except Exception as e:
        st.error(f"Error loading or processing routes.csv: {e}. Ensure it's a valid CSV with 'route_id' and 'route_short_name' columns.")
        return None

@st.cache_data(ttl=3600 * 24) # Cache for 24 hours
def load_static_stop_times_data(zip_url):
    """
    Downloads a stop_times.zip file from a URL, extracts stop_times.csv,
    and returns it as a Pandas DataFrame.
    """
    st.info(f"Attempting to load stop_times.csv from: {zip_url}")
    try:
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()

        zip_content = io.BytesIO(response.content)

        with zipfile.ZipFile(zip_content, 'r') as zf:
            try:
                with zf.open('stop_times.csv') as csv_file:
                    df = pd.read_csv(csv_file, sep=',')
                    # Ensure trip_id and stop_sequence are of comparable types
                    df['trip_id'] = df['trip_id'].astype(str)
                    df['stop_sequence'] = df['stop_sequence'].astype(int)
                    st.success("`stop_times.csv` loaded successfully from ZIP!")
                    return df
            except KeyError:
                st.error("Error: 'stop_times.csv' not found inside the zip file.")
                st.warning("Please ensure the CSV file is named 'stop_times.csv' and is at the root of the zip.")
                return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error downloading the stop_times.zip file from '{zip_url}': {e}")
        st.warning("Please check the URL and your internet connection.")
        return None
    except zipfile.BadZipFile:
        st.error(f"Error: The downloaded file from '{zip_url}' is not a valid zip file.")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred during stop_times.csv processing: {e}")
        return None


# --- Data Fetching and Processing ---

@st.cache_data(ttl=30) # Cache data for 30 seconds to reduce API calls
def fetch_and_process_data(static_stop_times_df=None, static_trips_map=None, static_routes_map=None):
    """
    Fetches data from the GTFS Realtime API and processes it into a DataFrame.
    Optionally calculates delay if static_stop_times_df is provided.
    Adds route_id from static_trips_map and route_short_name from static_routes_map if provided.
    """
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=10) # Added timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        records = []
        
        feed_header_version = feed.header.gtfs_realtime_version if feed.header.HasField("gtfs_realtime_version") else "Not Provided"
        feed_header_incrementality = feed.header.incrementality if feed.header.HasField("incrementality") else "Not Provided"
        feed_header_timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else "Not Provided"

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip_update = entity.trip_update
                trip = trip_update.trip
                vehicle = trip_update.vehicle

                trip_id = trip.trip_id if trip.HasField("trip_id") else "N/A"
                
                # Get route_id from static trips data
                route_id_s = "Not Found in Static Trips"
                if static_trips_map is not None and trip_id != "N/A":
                    route_id_s = static_trips_map.get(trip_id, "Not Found in Static Trips")

                # Get route_short_name from static routes data using route_id_s
                route_s = "Not Found in Static Routes"
                if static_routes_map is not None and route_id_s != "Not Found in Static Trips":
                    route_s = static_routes_map.get(route_id_s, "Not Found in Static Routes")

                route_parsed, direction_parsed = parse_trip_id(trip_id) 
                route_id = trip.route_id if trip.HasField("route_id") else "Not Provided"
                direction_id = trip.direction_id if trip.HasField("direction_id") else "Not Provided"
                start_date = trip.start_date if trip.HasField("start_date") else "Not Provided"
                start_time = trip.start_time if trip.HasField("start_time") else "Not Provided"
                trip_schedule_relationship = trip.schedule_relationship if trip.HasField("schedule_relationship") else "Not Provided"

                trip_update_timestamp = trip_update.timestamp if trip_update.HasField("timestamp") else "Not Provided"
                trip_delay = trip_update.delay if trip_update.HasField("delay") else "Not Provided"
                
                vehicle_id = vehicle.id if vehicle.HasField("id") else "N/A"

                for stop in trip_update.stop_time_update:
                    stop_sequence = stop.stop_sequence if stop.HasField("stop_sequence") else "N/A"
                    stop_id = stop.stop_id if stop.HasField("stop_id") else "N/A"
                    stop_time_update_schedule_relationship = stop.schedule_relationship if stop.HasField("schedule_relationship") else "Not Provided"

                    # --- Arrival Time and Delay Calculation ---
                    arrival_time_unix = None
                    stop_arrival_delay = "Not Provided"
                    stop_arrival_uncertainty = "Not Provided"
                    if stop.HasField("arrival"):
                        arrival_time_unix = stop.arrival.time if stop.arrival.HasField("time") else None
                        
                        if stop.arrival.HasField("delay"):
                            stop_arrival_delay = stop.arrival.delay
                        elif static_stop_times_df is not None and arrival_time_unix is not None:
                            # Attempt to calculate delay using static schedule
                            try:
                                # Find the scheduled arrival time for this trip and stop
                                # Ensure trip_id and stop_sequence match types in static_stop_times_df
                                scheduled_row = static_stop_times_df[
                                    (static_stop_times_df['trip_id'] == trip_id) &
                                    (static_stop_times_df['stop_sequence'] == stop_sequence)
                                ]
                                if not scheduled_row.empty:
                                    scheduled_arrival_time_str = scheduled_row['arrival_time'].iloc[0]
                                    scheduled_arrival_seconds = time_string_to_seconds(scheduled_arrival_time_str)

                                    if scheduled_arrival_seconds is not None:
                                        # Convert real-time Unix timestamp to seconds from midnight (local time)
                                        realtime_dt_utc = datetime.datetime.fromtimestamp(arrival_time_unix, tz=datetime.timezone.utc)
                                        realtime_dt_local = realtime_dt_utc + datetime.timedelta(hours=10) # Adjust to UTC+10
                                        realtime_arrival_seconds = (realtime_dt_local.hour * 3600 + 
                                                                    realtime_dt_local.minute * 60 + 
                                                                    realtime_dt_local.second)
                                        
                                        stop_arrival_delay = realtime_arrival_seconds - scheduled_arrival_seconds
                                    else:
                                        stop_arrival_delay = "Not Calculated (Invalid Scheduled Time Format)"
                                else:
                                    stop_arrival_delay = "Not Calculated (Static Schedule Entry Not Found)"
                            except Exception as e:
                                stop_arrival_delay = f"Calculation Error: {e}"
                        else:
                            stop_arrival_delay = "Not Provided" 

                        stop_arrival_uncertainty = stop.arrival.uncertainty if stop.arrival.HasField("uncertainty") else "Not Provided"
                    
                    # --- Departure Time and Delay Calculation ---
                    departure_time_unix = None
                    stop_departure_delay = "Not Provided"
                    stop_departure_uncertainty = "Not Provided"
                    if stop.HasField("departure"):
                        departure_time_unix = stop.departure.time if stop.departure.HasField("time") else None
                        
                        if stop.departure.HasField("delay"):
                            stop_departure_delay = stop.departure.delay
                        elif static_stop_times_df is not None and departure_time_unix is not None:
                            # Attempt to calculate delay using static schedule
                            try:
                                scheduled_row = static_stop_times_df[
                                    (static_stop_times_df['trip_id'] == trip_id) &
                                    (static_stop_times_df['stop_sequence'] == stop_sequence)
                                ]
                                if not scheduled_row.empty:
                                    scheduled_departure_time_str = scheduled_row['departure_time'].iloc[0]
                                    scheduled_departure_seconds = time_string_to_seconds(scheduled_departure_time_str)

                                    if scheduled_departure_seconds is not None:
                                        realtime_dt_utc = datetime.datetime.fromtimestamp(departure_time_unix, tz=datetime.timezone.utc)
                                        realtime_dt_local = realtime_dt_utc + datetime.timedelta(hours=10) # Adjust to UTC+10
                                        realtime_departure_seconds = (realtime_dt_local.hour * 3600 + 
                                                                    realtime_dt_local.minute * 60 + 
                                                                    realtime_dt_local.second)
                                        
                                        stop_departure_delay = realtime_departure_seconds - scheduled_departure_seconds
                                    else:
                                        stop_departure_delay = "Not Calculated (Invalid Scheduled Time Format)"
                                else:
                                    stop_departure_delay = "Not Calculated (Static Schedule Entry Not Found)"
                            except Exception as e:
                                stop_departure_delay = f"Calculation Error: {e}"
                        else:
                            stop_departure_delay = "Not Provided" 

                        stop_departure_uncertainty = stop.departure.uncertainty if stop.departure.HasField("uncertainty") else "Not Provided"

                    records.append({
                        "Feed GTFS Realtime Version": feed_header_version,
                        "Feed Incrementality": feed_header_incrementality,
                        "Feed Timestamp": convert_unix_to_time(feed_header_timestamp),
                        "Entity ID": entity_id,
                        "Entity Is Deleted": entity_is_deleted,
                        "Trip Update Timestamp": convert_unix_to_time(trip_update_timestamp),
                        "Trip Delay": trip_delay,
                        "Vehicle ID": vehicle_id,
                        "Trip ID": trip_id,
                        "Route (Parsed)": route_parsed,
                        "Direction (Parsed)": direction_parsed,
                        "Trip Route ID": route_id,
                        "Trip Direction ID": direction_id,
                        "Trip Start Date": start_date,
                        "Trip Start Time": start_time,
                        "Trip Schedule Relationship": trip_schedule_relationship,
                        "Stop ID": stop_id,
                        "Stop Sequence": stop_sequence,
                        "Stop Time Update Schedule Relationship": stop_time_update_schedule_relationship,
                        "Arrival Time": convert_unix_to_time(arrival_time_unix), 
                        "Stop Arrival Delay": stop_arrival_delay,
                        "Stop Arrival Uncertainty": stop_arrival_uncertainty,
                        "Departure Time": convert_unix_to_time(departure_time_unix), 
                        "Stop Departure Delay": stop_departure_delay,
                        "Stop Departure Uncertainty": stop_departure_uncertainty,
                        "route_id_s": route_id_s, 
                        "route_s": route_s, 
                    })
        return pd.DataFrame(records)
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred during data processing: {e}")
        return pd.DataFrame()

# --- Streamlit App Logic ---

# Load static trips data from GitHub
static_trips_map = load_static_trips_data()

# Load static routes data from GitHub
static_routes_map = load_static_routes_data()

# Load static stop_times data from GitHub ZIP
static_stop_times_df = load_static_stop_times_data(STOP_TIMES_ZIP_URL)

# Fetch and process real-time data, passing static data
df = fetch_and_process_data(static_stop_times_df, static_trips_map, static_routes_map)

if not df.empty:
    st.write(f"Data last updated: {datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10))).strftime('%H:%M:%S')} (UTC+10)") 

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Trips")

    all_routes = sorted(df["Route (Parsed)"].dropna().unique().tolist()) 
    if "Unknown" in all_routes:
        all_routes.remove("Unknown")
    if '903' in all_routes:
        default_route_index = all_routes.index('903')
    else:
        default_route_index = 0 

    selected_route = st.sidebar.selectbox(
        "Select Route",
        options=all_routes,
        index=default_route_index 
    )

    if selected_route == "All":
        filtered_df_for_directions = df
    else:
        filtered_df_for_directions = df[df["Route (Parsed)"] == selected_route] 

    all_directions = sorted(filtered_df_for_directions["Direction (Parsed)"].dropna().unique().tolist()) 
    if "Unknown" in all_directions:
        all_directions.remove("Unknown")
    all_directions.insert(0, "All") 
    
    default_direction_index = all_directions.index("All") if "All" in all_directions else 0
    selected_direction = st.sidebar.selectbox(
        "Select Direction",
        options=all_directions,
        index=default_direction_index 
    )

    if selected_route == "All":
        filtered_df_for_stops = df
    else:
        filtered_df_for_stops = df[df["Route (Parsed)"] == selected_route] 

    if selected_direction != "All":
        filtered_df_for_stops = filtered_df_for_stops[filtered_df_for_stops["Direction (Parsed)"] == selected_direction] 

    all_stops = filtered_df_for_stops["Stop Sequence"].dropna().unique().tolist()
    
    numeric_stops = [s for s in all_stops if isinstance(s, (int, float)) and s != "N/A"]
    non_numeric_stops = [s for s in all_stops if not isinstance(s, (int, float)) or s == "N/A"]
    
    all_stops_sorted = sorted(numeric_stops) + sorted(non_numeric_stops) 
    all_stops_sorted.insert(0, "All") 

    default_stop_index = all_stops_sorted.index("All") if "All" in all_stops_sorted else 0
    selected_stop_seq = st.sidebar.selectbox(
        "Select Stop Sequence",
        options=all_stops_sorted,
        index=default_stop_index 
    )

    # --- Apply Filters to DataFrame ---
    st.subheader("🚏 Filtered Trip Data")

    final_filtered_df = df.copy() 

    if selected_route != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Route (Parsed)"] == selected_route] 
    
    if selected_direction != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Direction (Parsed)"] == selected_direction] 
    
    if selected_stop_seq != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Stop Sequence"] == selected_stop_seq]


    if not final_filtered_df.empty:
        st.dataframe(final_filtered_df.reset_index(drop=True), use_container_width=True)
    else:
        st.warning("No matching records found for the selected filters.")
else:
    st.info("No data available to display. Please check API connectivity or try again later.")

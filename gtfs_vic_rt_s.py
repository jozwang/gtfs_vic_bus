import requests
import streamlit as st
import pandas as pd
from google.transit import gtfs_realtime_pb2
import datetime

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
        return datetime.datetime.fromtimestamp(adjusted_timestamp, tz=datetime.timezone.utc).strftime('%H:%MM:%S')
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
            else:
                direction = "Unknown"
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

# Updated static stop times URL
STATIC_STOP_TIMES_URL = "https://raw.githubusercontent.com/jozwang/gtfs_vic_bus/refs/heads/main/stop_times_box_hill_4.csv"

# --- Data Fetching and Processing ---

@st.cache_data(ttl=30) # Cache data for 30 seconds to reduce API calls
def fetch_and_process_data():
    """Fetches data from the GTFS Realtime API, static stop times, and processes it into a merged DataFrame."""
    try:
        # Fetch Realtime Data
        response = requests.get(base_url, headers=headers, params=params, timeout=10) # Added timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        records = []
        
        # Extract Feed Header information once (these fields will be duplicated per row in the DataFrame)
        feed_header_version = feed.header.gtfs_realtime_version if feed.header.HasField("gtfs_realtime_version") else "Not Provided"
        feed_header_incrementality = feed.header.incrementality if feed.header.HasField("incrementality") else "Not Provided"
        feed_header_timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else "Not Provided"

        for entity in feed.entity:
            # Entity-level fields
            entity_id = entity.id if entity.HasField("id") else "Not Provided"
            entity_is_deleted = entity.is_deleted if entity.HasField("is_deleted") else "Not Provided"

            if entity.HasField('trip_update'):
                trip_update = entity.trip_update
                trip = trip_update.trip
                vehicle = trip_update.vehicle

                # TripDescriptor fields (from trip_update.trip)
                trip_id = trip.trip_id if trip.HasField("trip_id") else "N/A"
                # Route and Direction parsed from trip_id for convenience
                route_parsed, direction_parsed = parse_trip_id(trip_id) 
                # Direct fields from TripDescriptor
                route_id = trip.route_id if trip.HasField("route_id") else "Not Provided"
                direction_id = trip.direction_id if trip.HasField("direction_id") else "Not Provided"
                start_date = trip.start_date if trip.HasField("start_date") else "Not Provided"
                start_time = trip.start_time if trip.HasField("start_time") else "Not Provided"
                trip_schedule_relationship = trip.schedule_relationship if trip.HasField("schedule_relationship") else "Not Provided"

                # TripUpdate fields
                trip_update_timestamp = trip_update.timestamp if trip_update.HasField("timestamp") else "Not Provided"
                trip_delay = trip_update.delay if trip_update.HasField("delay") else "Not Provided"
                
                # VehicleDescriptor fields (from trip_update.vehicle)
                vehicle_id = vehicle.id if vehicle.HasField("id") else "N/A"

                for stop in trip_update.stop_time_update:
                    # StopTimeUpdate fields
                    stop_sequence = stop.stop_sequence if stop.HasField("stop_sequence") else "N/A"
                    stop_id = stop.stop_id if stop.HasField("stop_id") else "N/A"
                    stop_time_update_schedule_relationship = stop.schedule_relationship if stop.HasField("schedule_relationship") else "Not Provided"

                    # StopTimeEvent - Arrival (from stop_time_update.arrival)
                    arrival_time = "N/A"
                    stop_arrival_delay = "Not Provided"
                    stop_arrival_uncertainty = "Not Provided"
                    if stop.HasField("arrival"):
                        arrival_time = stop.arrival.time if stop.arrival.HasField("time") else "N/A"
                        stop_arrival_delay = stop.arrival.delay if stop.arrival.HasField("delay") else "Not Provided"
                        stop_arrival_uncertainty = stop.arrival.uncertainty if stop.arrival.HasField("uncertainty") else "Not Provided"
                    
                    # StopTimeEvent - Departure (from stop_time_update.departure)
                    departure_time = "N/A"
                    stop_departure_delay = "Not Provided"
                    stop_departure_uncertainty = "Not Provided"
                    if stop.HasField("departure"):
                        departure_time = stop.departure.time if stop.departure.HasField("time") else "N/A"
                        stop_departure_delay = stop.departure.delay if stop.departure.HasField("delay") else "Not Provided"
                        stop_departure_uncertainty = stop.departure.uncertainty if stop.departure.HasField("uncertainty") else "Not Provided"

                    records.append({
                        # Feed Header Fields
                        "Feed GTFS Realtime Version": feed_header_version,
                        "Feed Incrementality": feed_header_incrementality,
                        "Feed Timestamp": convert_unix_to_time(feed_header_timestamp),

                        # Entity Fields
                        "Entity ID": entity_id,
                        "Entity Is Deleted": entity_is_deleted,

                        # TripUpdate Fields
                        "Trip Update Timestamp": convert_unix_to_time(trip_update_timestamp),
                        "Trip Delay": trip_delay,
                        "Vehicle ID": vehicle_id,

                        # TripDescriptor Fields (from trip_update.trip)
                        "trip_id": trip_id, # Changed to lowercase for easier joining
                        "Route (Parsed)": route_parsed, # Extracted by parsing the trip_id
                        "Direction (Parsed)": direction_parsed, # Extracted by parsing the trip_id
                        "Trip Route ID": route_id, # Directly from TripDescriptor
                        "Trip Direction ID": direction_id, # Directly from TripDescriptor
                        "Trip Start Date": start_date,
                        "Trip Start Time": start_time,
                        "Trip Schedule Relationship": trip_schedule_relationship,

                        # StopTimeUpdate Fields (from trip_update.stop_time_update)
                        "Stop ID": stop_id,
                        "stop_sequence": stop_sequence, # Changed to lowercase for easier joining
                        "Stop Time Update Schedule Relationship": stop_time_update_schedule_relationship,
                        
                        # StopTimeEvent - Arrival (from stop_time_update.arrival)
                        "Realtime Arrival Time": convert_unix_to_time(arrival_time), # Renamed for clarity
                        "Stop Arrival Delay": stop_arrival_delay,
                        "Stop Arrival Uncertainty": stop_arrival_uncertainty,

                        # StopTimeEvent - Departure (from stop_time_update.departure)
                        "Realtime Departure Time": convert_unix_to_time(departure_time), # Renamed for clarity
                        "Stop Departure Delay": stop_departure_delay,
                        "Stop Departure Uncertainty": stop_departure_uncertainty,
                    })
        realtime_df = pd.DataFrame(records)

        # Fetch Static Stop Times Data
        # Read stop_lat and stop_lon as strings first to clean them
        static_stop_times_df = pd.read_csv(
            STATIC_STOP_TIMES_URL,
            dtype={
                'trip_id': str,
                'stop_sequence': int,
                'route_id': str,
                'direction_id': str,
                'service_id': str,
                'trip_headsign': str,
                'stop_name': str,
                'stop_id': str,
                'stop_lat': str,  # Read as string initially
                'stop_lon': str,  # Read as string initially
                'departure_time': str 
            }
        )
        
        # Clean 'stop_lat' and 'stop_lon' columns by removing any leading/trailing non-numeric characters (like quotes)
        # and then convert to float. Using a regex to remove anything that's not a digit, a decimal point, or a minus sign.
        static_stop_times_df['stop_lat'] = static_stop_times_df['stop_lat'].astype(str).str.replace(r"[^\d.-]", "", regex=True).astype(float)
        static_stop_times_df['stop_lon'] = static_stop_times_df['stop_lon'].astype(str).str.replace(r"[^\d.-]", "", regex=True).astype(float)

        # Rename static columns to avoid conflicts and clarify origin
        static_stop_times_df = static_stop_times_df.rename(columns={
            'departure_time': 'Static Departure Time',
            'stop_id': 'Static Stop ID',
            'stop_headsign': 'Static Stop Headsign',
            'route_id': 'Static Route ID',
            'direction_id': 'Static Direction ID',
            'service_id': 'Static Service ID',
            'stop_name': 'Static Stop Name',
            'stop_lat': 'Static Stop Lat',
            'stop_lon': 'Static Stop Lon'
        })
        
        # Ensure 'stop_sequence' in realtime_df is integer for proper joining
        realtime_df['stop_sequence'] = pd.to_numeric(realtime_df['stop_sequence'], errors='coerce').fillna(-1).astype(int) # Use -1 for N/A sequences

        # Inner Join static_df with realtime_df on 'trip_id' and 'stop_sequence'
        merged_df = pd.merge(realtime_df, static_stop_times_df, on=['trip_id', 'stop_sequence'], how='inner')

        # Calculate Departure_in_Min
        now_utc10 = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10)))
        
        # Convert 'Realtime Departure Time' from 'HH:MM:SS' string to datetime.time objects
        merged_df['Realtime Departure Time Object'] = pd.to_datetime(merged_df['Realtime Departure Time'], format='%H:%M:%S', errors='coerce').dt.time

        def calculate_minutes_difference(departure_time_obj, current_time_obj):
            if pd.isna(departure_time_obj):
                return None
            
            # Create dummy dates for calculation, just focusing on time difference
            dummy_date = datetime.date(2000, 1, 1) # A common arbitrary date
            departure_datetime = datetime.datetime.combine(dummy_date, departure_time_obj)
            current_datetime = datetime.datetime.combine(dummy_date, current_time_obj)

            # If current time is past departure time, assume it's for the next day
            # This handles cases where departure time is for early next morning (e.g., 00:05 and current time is 23:50)
            if current_datetime > departure_datetime:
                departure_datetime += datetime.timedelta(days=1)
            
            diff = departure_datetime - current_datetime
            return diff.total_seconds() / 60

        merged_df['Departure_in_Min'] = merged_df['Realtime Departure Time Object'].apply(lambda x: calculate_minutes_difference(x, now_utc10.time()))
        
        # Drop the temporary column
        merged_df = merged_df.drop(columns=['Realtime Departure Time Object'])

        return merged_df
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        st.error(f"An unexpected error occurred during data processing: {e}")
        return pd.DataFrame()

df = fetch_and_process_data()

# --- Streamlit App Logic ---

if not df.empty:
    # Adjusted to show current time in UTC+10
    st.write(f"Data last updated: {datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10))).strftime('%H:%M:%S')} (UTC+10)") 

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Trips")

    ## 1. Default route filter is 903
    all_routes = sorted(df["Route (Parsed)"].dropna().unique().tolist()) 
    # Ensure '903' is in options, handle 'Unknown'
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

    # Filter directions based on selected route (or all if 'All' route selected)
    if selected_route == "All":
        filtered_df_for_directions = df
    else:
        filtered_df_for_directions = df[df["Route (Parsed)"] == selected_route] 

    ## 2. Allow "All" for all filters & 3. Default "All" for direction
    all_directions = sorted(filtered_df_for_directions["Direction (Parsed)"].dropna().unique().tolist()) 
    if "Unknown" in all_directions:
        all_directions.remove("Unknown")
    all_directions.insert(0, "All") 
    
    # Set default to 'All'
    default_direction_index = all_directions.index("All") if "All" in all_directions else 0
    selected_direction = st.sidebar.selectbox(
        "Select Direction",
        options=all_directions,
        index=default_direction_index 
    )

    # --- Apply Filters to DataFrame ---
    st.subheader("🚏 Filtered Trip Data")

    final_filtered_df = df.copy() 

    if selected_route != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Route (Parsed)"] == selected_route] 
    
    if selected_direction != "All":
        final_filtered_df = final_filtered_df[final_filtered_df["Direction (Parsed)"] == selected_direction] 
    
    if not final_filtered_df.empty:
        st.dataframe(final_filtered_df.reset_index(drop=True), use_container_width=True)
    else:
        st.warning("No matching records found for the selected filters.")
else:
    st.info("No data available to display. Please check API connectivity or try again later.")

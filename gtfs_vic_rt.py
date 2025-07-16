import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from tabulate import tabulate
import datetime
import zipfile
import io

# link to the GTFS Realtime API documentation:
# https://discover.data.vic.gov.au/dataset/gtfs-realtime/resource/d075af50-f8d9-449d-89bf-1899940c5e38

# --- Static Data URLs ---
STOP_TIMES_ZIP_URL = "https://raw.githubusercontent.com/jozwang/gtfs_vic_bus/refs/heads/main/stop_times_4_kmel.zip"

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

        # More robust direction parsing
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

def load_static_stop_times_data(zip_url):
    """
    Downloads a stop_times.zip file from a URL, extracts stop_times.csv,
    and returns it as a Pandas DataFrame.
    """
    print(f"Attempting to load stop_times.csv from: {zip_url}")
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
                    print("`stop_times.csv` loaded successfully from ZIP!")
                    return df
            except KeyError:
                print("Error: 'stop_times.csv' not found inside the zip file.")
                print("Please ensure the CSV file is named 'stop_times.csv' and is at the root of the zip.")
                return None
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the stop_times.zip file from '{zip_url}': {e}")
        print("Please check the URL and your internet connection.")
        return None
    except zipfile.BadZipFile:
        print(f"Error: The downloaded file from '{zip_url}' is not a valid zip file.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during stop_times.csv processing: {e}")
        return None


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

# --- Main Data Processing ---

# Load static stop_times data from GitHub ZIP
static_stop_times_df = load_static_stop_times_data(STOP_TIMES_ZIP_URL)

response = requests.get(base_url, headers=headers, params=params)

if response.status_code == 200:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records = []
    
    # Extract Feed Header information once (these fields will be duplicated per row in the DataFrame)
    feed_header_version = feed.header.gtfs_realtime_version if feed.header.HasField("gtfs_realtime_version") else "Not Provided"
    feed_header_incrementality = feed.header.incrementality if feed.header.HasField("incrementality") else "Not Provided"
    feed_header_timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else "Not Provided"

    for entity in feed.entity:
        # Initialize entity-level fields at the start of each entity loop iteration
        # This ensures they are always defined, even if 'trip_update' is not present
        entity_id = "Not Provided"
        entity_is_deleted = "Not Provided"

        if entity.HasField("id"):
            entity_id = entity.id
        if entity.HasField("is_deleted"):
            entity_is_deleted = entity.is_deleted

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
                        stop_arrival_delay = "Not Provided (Requires Static GTFS Data for Calculation)" 

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
                        stop_departure_delay = "Not Provided (Requires Static GTFS Data for Calculation)" 

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
                    
                    "Arrival Time": convert_unix_to_time(arrival_time_unix), # Use the unix timestamp for conversion
                    "Stop Arrival Delay": stop_arrival_delay,
                    "Stop Arrival Uncertainty": stop_arrival_uncertainty,

                    "Departure Time": convert_unix_to_time(departure_time_unix), # Use the unix timestamp for conversion
                    "Stop Departure Delay": stop_departure_delay,
                    "Stop Departure Uncertainty": stop_departure_uncertainty,
                })

    df = pd.DataFrame(records)
    
    # Filter for Route 903
    df_903 = df[df['Route (Parsed)'] == '903'] # Changed to 'Route (Parsed)'

    if not df_903.empty: 
        print(tabulate(df_903.head(), headers='keys', tablefmt='psql')) 
    else:
        print("No vehicle or trip updates currently available for Route 903.")

else:
    print(f"Failed to retrieve data: {response.status_code} - {response.text}")

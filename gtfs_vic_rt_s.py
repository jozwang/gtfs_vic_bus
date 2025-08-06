import requests
import streamlit as st
import pandas as pd
from google.transit import gtfs_realtime_pb2
import datetime
import pytz
import numpy as np
import redis # Import the redis library
from io import StringIO # Import StringIO for future-proof JSON reading

# --- Utility Functions ---

def convert_unix_to_time(unix_timestamp):
    """
    Converts a Unix timestamp to HH:MM:SS format, adjusted for UTC+10.
    Returns 'N/A' if invalid or None.
    """
    if unix_timestamp is None or not isinstance(unix_timestamp, (int, float)):
        return "N/A"
    try:
        melbourne_tz = pytz.timezone('Australia/Melbourne')
        utc_dt = datetime.datetime.fromtimestamp(unix_timestamp, tz=pytz.utc)
        melbourne_dt = utc_dt.astimezone(melbourne_tz)
        return melbourne_dt.strftime('%H:%M:%S')
    except (ValueError, TypeError):
        return "N/A"

# --- Streamlit Application Setup ---

st.set_page_config(page_title="Metro Bus Realtime Snapshot", layout="wide")

col1, col2 = st.columns([5,5])

with col1:
    st.title("🚍 PTV Metro Bus Realtime Snapshot – Box Hill")

with col2:
    st.image("SkyBus Powerpoint Template.jpg", use_container_width=False, width=300)

# --- API Configuration ---

api_key = st.secrets['API_key']
base_url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates"
headers = {"Ocp-Apim-Subscription-Key": api_key}
params = {"subscription-key": api_key}

# --- Data Fetching and Processing ---

@st.cache_data(ttl=30) # Cache data for 30 seconds
def fetch_and_process_data():
    """Fetches data from Redis (for static schedules) and the GTFS Realtime API, then processes it."""
    try:
        melbourne_tz = pytz.timezone('Australia/Melbourne')
        now_utc10 = datetime.datetime.now(melbourne_tz)
        today_date_int = int(now_utc10.strftime('%Y%m%d')) # <-- NEW: Define today's date as an integer
        
        # --- REVISED SECTION: Fetch Static Stop Times from Redis ---
        
        # 1. Connect to Redis and fetch today's schedules
        # Assumes REDIS_URL is stored in Streamlit's secrets
        r = redis.from_url(st.secrets["REDIS_URL"], decode_responses=True)
        schedules_json = r.get("schedules:box_hill:today_and_tomorrow")

        if not schedules_json:
            st.warning("Could not find today's schedule data in the Redis cache. Please ensure the daily job has run.")
            return pd.DataFrame()

        # 2. Load the JSON data into a DataFrame
        # The data from Redis is already filtered for today's services
        static_stop_times_df = pd.read_json(StringIO(schedules_json), orient='records')

          # --- NEW CODE: Filter for today's date before other processing ---
        original_record_count = len(static_stop_times_df)
        static_stop_times_df = static_stop_times_df[static_stop_times_df['date'] == today_date_int]
        st.info(f"Filtered schedules for today's date ({today_date_int}). {len(static_stop_times_df)} of {original_record_count} records remain.")
        
        if static_stop_times_df.empty:
            st.warning("No static trips found for today's date. The daily cron job may not have run, or there are no scheduled services for today.")
            return pd.DataFrame()

        # 3. Ensure data types are correct (matching the old script's expectations)
        static_stop_times_df['stop_lat'] = static_stop_times_df['stop_lat'].astype(float)
        static_stop_times_df['stop_lon'] = static_stop_times_df['stop_lon'].astype(float)
        static_stop_times_df['stop_sequence'] = static_stop_times_df['stop_sequence'].astype(int)
        
        # 4. Rename columns for display and consistency with the rest of the script
        static_stop_times_df = static_stop_times_df.rename(columns={
            'route_id': 'Static Route ID',
            'direction_id': 'Static Direction ID',
            'service_id': 'Static Service ID',
            'trip_headsign': 'Trip Headsign',
            'stop_name': 'Static Stop Name',
            'stop_id': 'Static Stop ID',
            'departure_time': 'Static Departure Time',
            'date':'Calendar Date',
            'route_short_name': 'Display Route Name'
        })


        # # 5. Remove rows in stop_times df if static stop departure time is 4 hours before or after current time.
        # today_date = now_utc10.date()


            
        # Fetch Realtime Data
        response = requests.get(base_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        records = []
        feed_header_timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else "Not Provided"

        for entity in feed.entity:
            if not entity.HasField('trip_update'):
                continue
            trip_update = entity.trip_update
            trip = trip_update.trip
            trip_id = trip.trip_id if trip.HasField("trip_id") else "N/A"
            start_date = trip.start_date if trip.HasField("start_date") else "Not Provided"
            start_time = trip.start_time if trip.HasField("start_time") else "Not Provided"
            
            for stop in trip_update.stop_time_update:
                records.append({
                    "Feed Timestamp": convert_unix_to_time(feed_header_timestamp), 
                    "Entity ID": entity.id, 
                    "trip_id": trip_id, 
                    "Trip Start Date": start_date,
                    "Trip Start Time": start_time,
                    "stop_sequence": stop.stop_sequence if stop.HasField("stop_sequence") else -1,
                    "Realtime Arrival Time": convert_unix_to_time(stop.arrival.time if stop.HasField("arrival") and stop.arrival.HasField("time") else None), 
                    "Realtime Departure Time": convert_unix_to_time(stop.departure.time if stop.HasField("departure") and stop.departure.HasField("time") else None), 
                })
        realtime_df = pd.DataFrame(records)
        realtime_df['stop_sequence'] = pd.to_numeric(realtime_df['stop_sequence'], errors='coerce').fillna(-1).astype(int)

        # ... right before the merge line
        st.subheader("Debugging: Static Data Preview")
        st.write(static_stop_times_df[['trip_id', 'stop_sequence', 'Static Departure Time', 'Calendar Date', 'Display Route Name']].sort_values(by=['trip_id', 'stop_sequence']).head())
        
        st.subheader("Debugging: Realtime Data Preview")
        st.write(realtime_df[['trip_id', 'stop_sequence', 'Realtime Departure Time', 'Trip Start Date']].sort_values(by=['trip_id', 'stop_sequence']).head())
        st.write(f"Realtime dataframe has {len(realtime_df)} records.")

        # Left join realtime data onto the static schedule
        merged_df = pd.merge(static_stop_times_df, realtime_df, on=['trip_id', 'stop_sequence'], how='inner')

           # --- Remove duplicate rows after the merge ---
        # Duplicates can be created if multiple real-time updates exist for a single static trip/stop.
        # This will keep the first instance it finds.
        merged_df.drop_duplicates(
            subset=['trip_id', 'stop_sequence', 'Static Departure Time', 'Trip Start Date'],
            keep='first',
            inplace=True
        )
        
        # Calculate departure minutes
        merged_df['Realtime Departure Time Object'] = merged_df['Realtime Departure Time'].apply(
            lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time() if isinstance(x, str) and x != "N/A" else None
        )
        
        def calculate_minutes_difference(departure_time_obj, current_full_datetime):
            if departure_time_obj is None or pd.isna(departure_time_obj):
                return np.nan
            
            departure_datetime_today = datetime.datetime.combine(
                current_full_datetime.date(), departure_time_obj, tzinfo=current_full_datetime.tzinfo
            )

            if departure_datetime_today < current_full_datetime:
                return np.nan
            
            diff = departure_datetime_today - current_full_datetime
            return diff.total_seconds() / 60

        merged_df['Departure_in_Min'] = merged_df['Realtime Departure Time Object'].apply(lambda x: calculate_minutes_difference(x, now_utc10))
        merged_df = merged_df.drop(columns=['Realtime Departure Time Object'])

        return merged_df
        
    except redis.exceptions.ConnectionError as e:
        st.error(f"Error connecting to Redis: {e}. Please check your connection details in Streamlit secrets.")
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching real-time data from API: {e}")
        return pd.DataFrame() 
    except Exception as e:
        st.error(f"An unexpected error occurred during data processing: {e}")
        return pd.DataFrame()

# --- Streamlit App Logic ---

if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

df = fetch_and_process_data()

if not df.empty:
    st.write(f"Data last updated: {datetime.datetime.now(pytz.timezone('Australia/Melbourne')).strftime('%H:%M:%S')} (UTC+10)") 

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Trips")
    temp_filtered_df = df.copy()

    all_stop_names = sorted(temp_filtered_df["Static Stop Name"].dropna().unique().tolist())
    all_stop_names.insert(0, "All")
    selected_stop_name = st.sidebar.selectbox("Stop Name", options=all_stop_names, index=0)
    if selected_stop_name != "All":
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Static Stop Name"] == selected_stop_name]

    all_routes = sorted(temp_filtered_df["Display Route Name"].dropna().unique().tolist())
    options_routes = ["All"] + all_routes
    selected_routes = st.sidebar.multiselect("Select Route(s)", options=options_routes, default=["All"])
    if "All" not in selected_routes and selected_routes:
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Display Route Name"].isin(selected_routes)]

    all_headsigns = sorted(temp_filtered_df["Trip Headsign"].dropna().unique().tolist())
    options_headsigns = ["All"] + all_headsigns
    selected_headsigns = st.sidebar.multiselect("Select Trip Headsign(s)", options=options_headsigns, default=["All"])
    if "All" not in selected_headsigns and selected_headsigns:
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Trip Headsign"].isin(selected_headsigns)]

    all_directions = sorted(temp_filtered_df["Static Direction ID"].dropna().unique().tolist())
    all_directions.insert(0, "All")
    selected_direction_id = st.sidebar.selectbox("Select Direction ID", options=all_directions, index=0)
    if selected_direction_id != "All":
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Static Direction ID"] == selected_direction_id]

    # --- Apply Filters to DataFrame ---
    st.subheader("🚏 Filtered Trip Data")
    final_filtered_df = temp_filtered_df.copy()
    final_filtered_df = final_filtered_df.dropna(subset=['Departure_in_Min'])
    final_filtered_df['Departure_in_Min'] = final_filtered_df['Departure_in_Min'].astype(int)
    final_filtered_df = final_filtered_df.sort_values(by=["Trip Headsign", "Realtime Departure Time"], ascending=[True, True])

    if not final_filtered_df.empty:
        grouped_trips = final_filtered_df.groupby('Trip Headsign')
        for headsign, group in grouped_trips:
            st.markdown(f"### Towards {headsign}")
            group_sorted = group.sort_values(by="Realtime Departure Time", ascending=True)
            for index, row in group_sorted.iterrows():
                with st.container(border=True):
                    col_route, col_destination, col_scheduled_time, col_estimated_time, col_mins_away = st.columns([1, 3, 1.5, 1.5, 1.5])
                    with col_route:
                        st.markdown(f'<div style="background-color: #f0f2f6; border-radius: 5px; padding: 8px 12px; text-align: center; font-weight: bold; font-size: 1.1em; color: #31333F; margin-top: 5px;">{row["Display Route Name"]}</div>', unsafe_allow_html=True)
                    with col_destination:
                        st.write(f"**To {row['Trip Headsign']}**")
                    with col_scheduled_time:
                        st.markdown(f"<small>Scheduled:</small>", unsafe_allow_html=True)
                        st.write(f"**{row['Static Departure Time']}**")
                    with col_estimated_time:
                        st.markdown(f"<small>Estimated Departure:</small>", unsafe_allow_html=True)
                        st.write(f"**{row['Realtime Departure Time']}**")
                    with col_mins_away:
                        mins_away = row['Departure_in_Min']
                        if pd.notna(mins_away):
                            st.markdown(f'<div style="border: 2px dotted #888888; border-radius: 5px; padding: 8px 12px; text-align: center; font-weight: bold; font-size: 1.1em; color: #31333F; margin-top: 5px;">{mins_away} mins</div>', unsafe_allow_html=True)
                        else:
                            st.write("Departed/N/A")
    else:
        st.warning("No matching records found for the selected filters. Please try adjusting the filters or refreshing the data.")
else:
    st.info("No data available to display. Please check the data sources or try again later.")

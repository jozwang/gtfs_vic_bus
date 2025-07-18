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
        # Corrected format: use %M for minutes, not %MM
        return datetime.datetime.fromtimestamp(adjusted_timestamp, tz=datetime.timezone.utc).strftime('%H:%M:%S')
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
        pass
    return route, direction

# --- Streamlit Application Setup ---

st.set_page_config(page_title="Metro Bus Realtime Snapshot", layout="wide")

# Create two columns for the header section
col1, col2 = st.columns([5,5]) # Creates two columns with equal width (5/5 and 5/5)

with col1:
    st.title("🚍 PTV Metro Bus Realtime Snapshot – Box Hill")

with col2:
    # Assuming 'SkyBus Powerpoint Template.jpg' is in the root of your GitHub repo
    st.image("SkyBus Powerpoint Template.jpg", use_container_width=False, width=300)

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

            # TripDescriptor fields
            trip_id = trip.trip_id if trip.HasField("trip_id") else "N/A"
            route_parsed, direction_parsed = parse_trip_id(trip_id)
            start_date = trip.start_date if trip.HasField("start_date") else "Not Provided"
            start_time = trip.start_time if trip.HasField("start_time") else "Not Provided"
            
            for stop in trip_update.stop_time_update:
                stop_sequence = stop.stop_sequence if stop.HasField("stop_sequence") else "N/A"
                
                arrival_time = "N/A"
                if stop.HasField("arrival"):
                    arrival_time = stop.arrival.time if stop.arrival.HasField("time") else "N/A"
                
                departure_time = "N/A"
                if stop.HasField("departure"):
                    departure_time = stop.departure.time if stop.departure.HasField("time") else "N/A"

                records.append({
                    "Feed Timestamp": convert_unix_to_time(feed_header_timestamp), 
                    "Entity ID": entity.id, 
                    "trip_id": trip_id, 
                    "Route (Parsed)": route_parsed, 
                    "Direction (Parsed)": direction_parsed, 
                    "Trip Start Date": start_date,
                    "Trip Start Time": start_time,
                    "stop_sequence": stop_sequence,
                    "Realtime Arrival Time": convert_unix_to_time(arrival_time), 
                    "Realtime Departure Time": convert_unix_to_time(departure_time), 
                })
        realtime_df = pd.DataFrame(records)

        # Fetch Static Stop Times Data
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
                'stop_lat': str, 
                'stop_lon': str, 
                'departure_time': str 
            }
        )
        
        static_stop_times_df['stop_lat'] = static_stop_times_df['stop_lat'].astype(str).str.replace(r"[^\d.-]", "", regex=True).astype(float)
        static_stop_times_df['stop_lon'] = static_stop_times_df['stop_lon'].astype(str).str.replace(r"[^\d.-]", "", regex=True).astype(float)

        static_stop_times_df = static_stop_times_df.rename(columns={
            'route_id': 'Static Route ID',
            'direction_id': 'Static Direction ID',
            'service_id': 'Static Service ID',
            'trip_headsign': 'Trip Headsign',
            'stop_name': 'Static Stop Name',
            'stop_id': 'Static Stop ID',
            'departure_time': 'Static Departure Time',
        })
        
        realtime_df['stop_sequence'] = pd.to_numeric(realtime_df['stop_sequence'], errors='coerce').fillna(-1).astype(int) 

        merged_df = pd.merge(realtime_df, static_stop_times_df, on=['trip_id', 'stop_sequence'], how='inner')

        now_utc10 = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10)))
        
        # Convert 'Realtime Departure Time' to datetime.time objects for calculation
        merged_df['Realtime Departure Time Object'] = pd.to_datetime(merged_df['Realtime Departure Time'], format='%H:%M:%S', errors='coerce').dt.time

        def calculate_minutes_difference(departure_time_obj, current_full_datetime):
            if pd.isna(departure_time_obj):
                return None
            
            # Combine current date with departure time.
            # Make it timezone-aware using the same timezone as current_full_datetime
            departure_datetime_today = datetime.datetime.combine(
                current_full_datetime.date(), departure_time_obj, tzinfo=current_full_datetime.tzinfo
            )

            # If the departure time is earlier than the current time, return None
            if departure_datetime_today < current_full_datetime:
                return None
            
            diff = departure_datetime_today - current_full_datetime
            return diff.total_seconds() / 60

        # Pass the full now_utc10 datetime object to the function
        merged_df['Departure_in_Min'] = merged_df['Realtime Departure Time Object'].apply(lambda x: calculate_minutes_difference(x, now_utc10))
        
        merged_df = merged_df.drop(columns=['Realtime Departure Time Object'])

        return merged_df
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return pd.DataFrame() 
    except Exception as e:
        st.error(f"An unexpected error occurred during data processing: {e}")
        return pd.DataFrame()

# --- Streamlit App Logic ---

# Add a refresh button to clear the cache and re-run the script
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun() # Rerun the script to re-fetch data

df = fetch_and_process_data()

if not df.empty:
    st.write(f"Data last updated: {datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10))).strftime('%H:%M:%S')} (UTC+10)") 

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Trips")

    # Initialize a temporary filtered DataFrame for cascading filters
    temp_filtered_df = df.copy()

    # 1. Stop Name Filter
    all_stop_names = sorted(temp_filtered_df["Static Stop Name"].dropna().unique().tolist())
    all_stop_names.insert(0, "All")
    selected_stop_name = st.sidebar.selectbox(
        "Stop Name",
        options=all_stop_names,
        index=0
    )
    if selected_stop_name != "All":
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Static Stop Name"] == selected_stop_name]


    # 2. Route Filter (cascading from Stop Name)
    all_routes = sorted(temp_filtered_df["Route (Parsed)"].dropna().unique().tolist()) 
    if "Unknown" in all_routes:
        all_routes.remove("Unknown")
    all_routes.insert(0, "All")
    
    default_route_index = all_routes.index("All") if "All" in all_routes else 0 

    selected_route = st.sidebar.selectbox(
        "Select Route",
        options=all_routes,
        index=default_route_index 
    )
    if selected_route != "All":
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Route (Parsed)"] == selected_route]


    # 3. Trip Headsign Filter (cascading from Stop Name and Route)
    all_headsigns = sorted(temp_filtered_df["Trip Headsign"].dropna().unique().tolist())
    all_headsigns.insert(0, "All")
    selected_headsign = st.sidebar.selectbox(
        "Select Trip Headsign",
        options=all_headsigns,
        index=0
    )
    if selected_headsign != "All":
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Trip Headsign"] == selected_headsign]

    # 4. Static Direction ID Filter (cascading from Stop Name, Route, and Trip Headsign)
    all_directions = sorted(temp_filtered_df["Static Direction ID"].dropna().unique().tolist())
    all_directions.insert(0, "All")
    selected_direction_id = st.sidebar.selectbox(
        "Select Direction ID",
        options=all_directions,
        index=0
    )
    if selected_direction_id != "All":
        temp_filtered_df = temp_filtered_df[temp_filtered_df["Static Direction ID"] == selected_direction_id]


    # --- Apply Filters to DataFrame ---
    st.subheader("🚏 Filtered Trip Data")

    # Assign the fully filtered temp_filtered_df to final_filtered_df
    final_filtered_df = temp_filtered_df.copy() 

    # Filter out rows where Departure_in_Min is None (i.e., bus has already departed)
    final_filtered_df = final_filtered_df.dropna(subset=['Departure_in_Min'])
    # Ensure Departure_in_Min is an integer for display
    final_filtered_df['Departure_in_Min'] = final_filtered_df['Departure_in_Min'].astype(int)

    # Sort the DataFrame by "Trip Headsign" then "Realtime Departure Time" for better grouping and ordering
    final_filtered_df = final_filtered_df.sort_values(by=["Trip Headsign", "Realtime Departure Time"], ascending=[True, True])

    if not final_filtered_df.empty:
        # Group by 'Trip Headsign' for PID-like display
        grouped_trips = final_filtered_df.groupby('Trip Headsign')

        for headsign, group in grouped_trips:
            st.markdown(f"### Towards {headsign}") # Display the heading for the group

            # Sort within each group by Realtime Departure Time for accurate display
            group_sorted = group.sort_values(by="Realtime Departure Time", ascending=True)

            for index, row in group_sorted.iterrows():
                # Use st.container() for each trip entry to visually group elements
                with st.container(border=True): # Adds a subtle border around each trip entry
                    # Create columns for each piece of information
                    # Adjust ratios to give more space to destination and less to route number/time
                    col_route, col_destination, col_scheduled_time, col_estimated_time, col_mins_away = st.columns([1, 3, 1.5, 1.5, 1.5]) 

                    with col_route:
                        # Styling for route number like a badge
                        st.markdown(
                            f"""
                            <div style="
                                background-color: #f0f2f6; 
                                border-radius: 5px; 
                                padding: 8px 12px; 
                                text-align: center; 
                                font-weight: bold; 
                                font-size: 1.1em; 
                                color: #31333F;
                                margin-top: 5px;
                            ">
                                {row['Route (Parsed)']}
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                    with col_destination:
                        st.write(f"**To {row['Trip Headsign']}**")
                        # st.markdown(f"<small>Scheduled: {row['Static Departure Time']}</small>", unsafe_allow_html=True)
                    
                    with col_scheduled_time:
                        st.markdown(f"<small>Scheduled:</small>", unsafe_allow_html=True)
                        st.write(f"**{row['Static Departure Time']}**")
                    
                    with col_estimated_time:
                        st.markdown(f"<small>Estimated Departure:</small>", unsafe_allow_html=True)
                        st.write(f"**{row['Realtime Departure Time']}**") # Display realtime as the main time
                    
                    with col_mins_away:
                        mins_away = row['Departure_in_Min']
                        if pd.notna(mins_away):
                            # Dotted border box for "Departure in Min"
                            st.markdown(
                                f"""
                                <div style="
                                    border: 2px dotted #888888; 
                                    border-radius: 5px; 
                                    padding: 8px 12px; 
                                    text-align: center; 
                                    font-weight: bold; 
                                    font-size: 1.1em; 
                                    color: #31333F;
                                    margin-top: 5px;
                                ">
                                    {mins_away} mins
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
                        else:
                            st.write("Departed") # This case should be rare due to dropna, but good for robustness
                # No horizontal rule if using st.container(border=True)
                # st.markdown("---") # Separator between trips

    else:
        st.warning("No matching records found for the selected filters. Please try adjusting the filters or refreshing the data.")
else:
    st.info("No data available to display. Please check API connectivity or try again later.")

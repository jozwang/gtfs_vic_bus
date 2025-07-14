import requests
from google.transit import gtfs_realtime_pb2

# API endpoint for Metro Bus Trip Updates
url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates"

# Make the request
response = requests.get(url)

if response.status_code == 200:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            print(f"Trip ID: {trip.trip_id}")
            print(f"Route ID: {trip.route_id}")
            print("---")
else:
    print(f"Failed to fetch data: {response.status_code}")

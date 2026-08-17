# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

# Track Walrus storage related events on the Sui blockchain

import datetime

# Std lib imports
import requests
import re

from utils import num_to_blob_id, GRAPHQL_URL, PATH_TO_WALRUS_CONFIG

system_object_id = re.findall(
    r"system_object:[ ]*(.*)", open(PATH_TO_WALRUS_CONFIG).read()
)[0]
print(f"System object ID: {system_object_id}")

# Query the Walrus system object on Sui through the GraphQL API
query = """
query ($objectId: SuiAddress!) {
    object(address: $objectId) {
        asMoveObject {
            contents { type { repr } }
        }
    }
}
"""
response = requests.post(
    GRAPHQL_URL,
    json={"query": query, "variables": {"objectId": system_object_id}},
)
assert response.status_code == 200

object_type = response.json()["data"]["object"]["asMoveObject"]["contents"]["type"][
    "repr"
]
walrus_package = re.findall("(0x[0-9a-f]+)::system", object_type)[0]
print(f"Walrus type: {walrus_package}")

# Query the latest 50 events emitted by the Walrus package (the maximum page size)
query = """
query ($eventType: String!) {
    events(last: 50, filter: { type: $eventType }) {
        nodes {
            timestamp
            contents { type { repr } json }
            transaction { digest }
        }
    }
}
"""
response = requests.post(
    GRAPHQL_URL,
    # Filtering by the `<package>::events` prefix matches all Walrus event types
    json={"query": query, "variables": {"eventType": f"{walrus_package}::events"}},
)
assert response.status_code == 200

events = response.json()["data"]["events"]["nodes"]
# Print the most recent events first
for event in reversed(events):
    # Parse the Walrus event
    tx_digest = event["transaction"]["digest"]
    event_type = event["contents"]["type"]["repr"].split("::")[-1]
    parsed_event = event["contents"]["json"]
    time_date = datetime.datetime.fromisoformat(
        event["timestamp"].replace("Z", "+00:00")
    )

    # Blob lifecycle events carry a blob ID; epoch events do not
    blob_id_num = parsed_event.get("blob_id")
    blob_id = num_to_blob_id(int(blob_id_num)) if blob_id_num is not None else ""

    # For registered blobs get their size in bytes
    if event_type == "BlobRegistered":
        size = f"{parsed_event['size']}B"
    else:
        size = ""

    print(f"{time_date} {event_type:<15} {size:>10} {blob_id} Tx:{tx_digest:<48}")

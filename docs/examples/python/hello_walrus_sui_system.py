# Author: Walrus Foundation
# License: Apache-2.0

# Example of querying the Walrus system object on Sui.

# This script demonstrates how to retrieve and display information about the Walrus
# decentralized storage system by querying the system object on the Sui blockchain.

# The Walrus system object uses a new architecture where the actual system state
# is stored in dynamic fields, requiring a two-step process:
# 1. Get the system object to obtain its version
# 2. Query the dynamic field with that version to get the actual system state

import requests
import re
from typing import Dict, Any, Optional

from utils import PATH_TO_WALRUS_CONFIG

# Configuration
FULLNODE_URL = "https://fullnode.mainnet.sui.io:443"
SYSTEM_OBJECT_ID = re.findall(
    r"system_object:[ ]*(.*)", open(PATH_TO_WALRUS_CONFIG).read()
)[0]

def make_rpc_request(method: str, params: list, request_id: int = 1) -> Dict[str, Any]:
    """
    Make a JSON-RPC request to the Sui fullnode.
    
    Args:
        method: The RPC method name
        params: The parameters for the RPC call
        request_id: The request ID (default: 1)
    
    Returns:
        The JSON response from the RPC call
    
    Raises:
        SystemExit: If the request fails or returns an error
    """
    request_data = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }
    
    response = requests.post(FULLNODE_URL, json=request_data)
    
    if response.status_code != 200:
        print(f"HTTP Error: {response.status_code}")
        exit(1)
    
    response_json = response.json()
    
    if "error" in response_json:
        print(f"RPC Error: {response_json['error']}")
        exit(1)
    
    if "result" not in response_json:
        print("Error: No result in response")
        exit(1)
    
    return response_json["result"]

def get_object(object_id: str, show_content: bool = True) -> Dict[str, Any]:
    """
    Get a Sui object by its ID.
    
    Args:
        object_id: The object ID to retrieve
        show_content: Whether to include object content in the response
    
    Returns:
        The object data
    """
    params = [
        object_id,
        {
            "showType": True,
            "showOwner": False,
            "showPreviousTransaction": False,
            "showDisplay": False,
            "showContent": show_content,
            "showBcs": False,
            "showStorageRebate": False,
        },
    ]
    
    result = make_rpc_request("sui_getObject", params)
    return result["data"]

def get_dynamic_fields(parent_object_id: str) -> list:
    """
    Get all dynamic fields for a parent object.
    
    Args:
        parent_object_id: The parent object ID
    
    Returns:
        List of dynamic fields
    """
    params = [
        parent_object_id,
        None,  # cursor
        None   # limit
    ]
    
    result = make_rpc_request("suix_getDynamicFields", params)
    return result["data"]

def find_system_state_field(dynamic_fields: list, version: int) -> Optional[str]:
    """
    Find the dynamic field containing the system state for the given version.
    
    Args:
        dynamic_fields: List of dynamic fields
        version: The system object version to look for
    
    Returns:
        The object ID of the dynamic field, or None if not found
    """
    for field in dynamic_fields:
        if (field["name"]["type"] == "u64" and 
            int(field["name"]["value"]) == version):
            return field["objectId"]
    return None

def get_system_state() -> Dict[str, Any]:
    """
    Retrieve the current Walrus system state.
    
    Returns:
        Dictionary containing the system state information
    """
    print(f"System object ID: {SYSTEM_OBJECT_ID}")
    
    # Get the system object to obtain its version
    system_object = get_object(SYSTEM_OBJECT_ID)
    version = int(system_object["content"]["fields"]["version"])
    print(f"System object version: {version}")
    
    # Get dynamic fields and find the one with the current version
    dynamic_fields = get_dynamic_fields(SYSTEM_OBJECT_ID)
    field_object_id = find_system_state_field(dynamic_fields, version)
    
    if field_object_id is None:
        print(f"Error: Could not find dynamic field with version {version}")
        exit(1)
    
    # Get the dynamic field object containing the system state
    dynamic_field_object = get_object(field_object_id)
    system_state_inner = dynamic_field_object["content"]["fields"]["value"]["fields"]
    
    return system_state_inner

def display_system_info(system_state: Dict[str, Any]) -> None:
    """
    Display the Walrus system information in a formatted way.
    
    Args:
        system_state: The system state dictionary
    """
    committee = system_state["committee"]["fields"]
    
    print("\n=== Walrus System Information ===")
    print(f"Current epoch: {committee['epoch']}")
    print(f"Committee members: {len(committee['members'])}")
    print(f"Number of shards: {committee['n_shards']}")
    print(f"Storage price per unit: {system_state['storage_price_per_unit_size']} MIST")
    print(f"Write price per unit: {system_state['write_price_per_unit_size']} MIST")
    print(f"Total capacity: {int(system_state['total_capacity_size']):,} bytes")
    print(f"Used capacity: {int(system_state['used_capacity_size']):,} bytes")
    
    # Calculate usage percentage
    total_capacity = int(system_state['total_capacity_size'])
    used_capacity = int(system_state['used_capacity_size'])
    usage_percentage = (used_capacity / total_capacity) * 100 if total_capacity > 0 else 0
    print(f"Capacity usage: {usage_percentage:.2f}%")

def main():
    """Main function to demonstrate querying the Walrus system object."""
    try:
        system_state = get_system_state()
        display_system_info(system_state)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(1)

if __name__ == "__main__":
    main()

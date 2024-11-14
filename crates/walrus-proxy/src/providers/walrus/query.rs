// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO: Reuse calls from `walrus-sui` here (#1170).
// TODO: Include the previous and next committees (#1174).

use std::vec;

use anyhow::Error;
use futures::future::try_join_all;

// The process to obtain a list of nodes we will speak to is onerous. At a high
// level, we start with the staking objectID for the network itself. This is a
// well known objectID and forms your root query.  From there, you may resolve
// the list of nodes that are allowed to send us metric information.

// 1.
// curl -s https://public-rpc.testnet.sui.io:443 \
// -H 'Content-Type: application/json' \
// -d '{
//   "jsonrpc": "2.0",
//   "id": 1,
//   "method": "suix_getDynamicFields",
//   "params":
// ["0x37c0e4d7b36a2f64d51bba262a1791f844cfd88f31379f1b7c04244061d43914"]
// }' | jq '.result.data[] | {objectId: .objectId, objectType: .objectType}'
// {
//     "jsonrpc": "2.0",
//     "result": {
//         "data": [
//             {
//                 "name": {
//                     "type": "u64",
//                     "value": "0"
//                 },
//                 "bcsName": "11111111",
//                 "type": "DynamicObject",
//                 "objectType": <snip>::staking_inner::StakingInnerV1",
//                 "objectId":
// "0xf3814a38466649d1da66877ae66ba14514f7cd1c7f3ab1dfa8e9f660422db9c5",
//                 "version": 182019998,
//                 "digest": "7wcTjAKwz6hqNnMS6XarJEUStYRHVH9zzpwfBewkfbP"
//             }
//         ],
//         "nextCursor":
//  "0xcfb0fa1c845789a8885cd3ce361eabbd5974cf18f311324a0232eb4f2ea82da2",
//         "hasNextPage": false
//     },
//     "id": 1
// }

// 2. take objectID:
//    0xf3814a38466649d1da66877ae66ba14514f7cd1c7f3ab1dfa8e9f660422db9c5
// 3. collect all
//    .result.data.content.fields.committee.fields.pos0.fields.contents[].
//    fields.key
// curl -s https://public-rpc.testnet.sui.io:443 \
// -H 'Content-Type: application/json' \
// -d '{
//   "jsonrpc": "2.0",
//   "id": 1,
//   "method": "sui_getObject",
//   "params":
// ["0xf3814a38466649d1da66877ae66ba14514f7cd1c7f3ab1dfa8e9f660422db9c5",
// {"showContent": true}] }' | jq '.result.data.content.fields.committee.fields.
// pos0.fields.contents[].fields.key'

// "0xcf4b9402e7f156bc75082bc07581b0829f081ccfc8c444c71df4536ea33d094a"
// "0x70bc82baec578437bf2f61ce024c2b6da46038ddbcb95dbfc72a2151103a8097"
// "0xdf1e267ad3f9753ce4050557fe53c0fbbfac848a514e62c6dd1e245e46fc11e4"
// "0x561f6477a3363ec4e73e3109dea5677eab00612276261e7c4861a80ebe388419"
// "0xd4f59a8eb8093f4754ebd9bbd99f0547ca850c59d5867c49145dec961a6e1ded"
// "0x75ce5818f2be06a3509107c2b789eb0a6bb1d948edc9c477a1ac56068986d63e"
// "0x86d3037445466e671e42cf4dd39c7cffe6b8245bc412b94ba3d862d7c714ffb0"
// "0x8e6a31892c6343b67717df8ca33103f1c944c6a2c40ed39c2b37ba3e221bfea1"
// "0xee7d289f87304e5a143e1c9ded9c2de33bc59af002d20131915fd3191a8365b0"
// "0xb854d64581de9d62a90e1b35ef6176c2ca5757e172c1676861083878c1947566"
// "0x6b680e1db2d4929c594ba341f85f8318946c4f33f1368bb2467726bc3e0dcf07"
// "0x4db2a532fd9e9a937ec9f042d0309bd95fc16bece162985d463c40ed73369aa4"
// "0xc7575ad69cc693bfa3c7b847c0ba4f233c7279aa91cbb64564ba1366d91a5dd8"
// "0xc63379c87bc8348bcecad36eecf3441db1c92ac1a977f83e89ff6d30379f6d87"
// "0x68e87ff66973cc3d31ac157f07380da0c04a7f2b2977cda8dd173d10f40f03a4"
// "0x565ee6d84664009c8019af4631de50c3aabb39f06795a8b5ea0c868c66c54261"
// "0x4c5d0e642776b7fc95a1b405ae566b816f14f14e1c653b06403e4e2b7c3d0f83"
// "0xb49af3dc71a4d8bf6ffdd8f8480ad092aa8fe7dbd7a65df60baad4df7b290383"
// "0x97fe244dd9d2ac46e13053f70d365c081255c76f8b9b030e3cdd3958ac42df0d"
// "0x1e6fb1b96f73fed6f10bee35e4e9bb4092767fee99764703123c125c95f57676"
// "0x581d2dcdcb3b39517bf2618e7f1f7a56adcad75a33a614a4965863d71e066c71"
// "0xa33f1831b47c5a69fa184c2c9103418162038298f17320a15873f4b4bdd18b5f"
// "0xc2d2c3b90c6f898b818ea7bf194db92f5ea0ac31d046860dadd992baab528d4a"
// "0x12dfbf6f33863e22ed3f4bf15c8ece6491fa6e8c9f1739ea3a64cc6c91b1ddea"
// "0x0128b44e4a79d1e92877b4482cb28043313ea600c789b801089bb568e4a9abc2"

// 4. iter over all keys and fetch name and pub key bytes
// curl -s https://public-rpc.testnet.sui.io:443 \
// -H 'Content-Type: application/json' \
// -d '{
//   "jsonrpc": "2.0",
//   "id": 1,
//   "method": "sui_getObject",
//   "params":
// ["0xcf4b9402e7f156bc75082bc07581b0829f081ccfc8c444c71df4536ea33d094a",
// {"showContent": true}] }' | jq '.result.data.content.fields.node_info.fields
// | {name, network_address, network_public_key}'

// now save {name: String, network_address: String, network_public_key: Vec<u8>}
// and you have your nodes!

/// get_well_known_staking_object_id depends on the well known staking object.
/// this forms the basis for all queries in this module. The staking_object_id
/// is exposed in yaml as a config item.
async fn get_well_known_staking_object_id(
    url: &str,
    staking_object_id: &str,
) -> Result<String, Error> {
    let method = "suix_getDynamicFields";
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method":method,
        "id":1,
        "params": vec![staking_object_id]
    });
    let response = reqwest::Client::new()
        .post(url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let error_message = response.text().await?;
        return Err(anyhow::anyhow!("query call failed; {error_message}"));
    }

    // Parse the response as JSON
    let response_json: serde_json::Value = response.json().await.map_err(|e| dbg!(e))?;

    const WANTED_OBJECT_TYPE: &str = "StakingInnerV1";
    // Filter the result to get only objectId and objectType
    let Some(data) = response_json["result"]["data"].as_array() else {
        return Err(anyhow::anyhow!(
            "unable to query for staking object.  \
method: {method} staking_object_id: {staking_object_id} \
staking_object_type: {WANTED_OBJECT_TYPE}"
        ));
    };
    // we found data, now ensure it is the data we want
    for item in data {
        // Check if objectId and objectType exist
        let (Some(object_id), Some(object_type)) = (item.get("objectId"), item.get("objectType"))
        else {
            continue;
        };
        // validate it is the object type we want by hoping it contains
        // WANTED_OBJECT_TYPE
        if !object_type
            .as_str()
            .unwrap_or_default()
            .contains(WANTED_OBJECT_TYPE)
        {
            continue;
        }
        return object_id
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("expected object_id to be a string, but it was not"))
            .map(|id| id.to_string());
    }
    Err(anyhow::anyhow!(
        "staking object did not return data that we wanted.  \
method: {method} staking_object_id: {staking_object_id} \
staking_object_type: {WANTED_OBJECT_TYPE}"
    ))
}

/// get_committee will fetch the node object_ids from the well known
/// staking_object_id, returning a list of object_ids to further resolve
async fn get_committee(url: &str, dynamic_field_object_id: &str) -> Result<Vec<String>, Error> {
    let method = "sui_getObject";
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method":method,
        "id":1,
        "params": [dynamic_field_object_id, {"showContent": true}]
    });
    let response = reqwest::Client::new()
        .post(url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let error_message = response.text().await?;
        return Err(anyhow::anyhow!("query call failed; {error_message}"));
    }

    // Parse the response as JSON
    let response_json: serde_json::Value = response.json().await?;
    // Extract the keys from the nested structure
    let Some(contents) = response_json["result"]["data"]["content"]["fields"]["committee"]
        ["fields"]["pos0"]["fields"]["contents"]
        .as_array()
    else {
        return Err(anyhow::anyhow!(
            "unable to find committee data in json response.  \
method: {method} dynamic_field_object_id: {dynamic_field_object_id}"
        ));
    };
    let mut object_ids = vec![];
    for item in contents {
        if let Some(object_id) = item["fields"]["key"].as_str() {
            object_ids.push(object_id.to_string());
        }
    }
    Ok(object_ids)
}

/// NodeInfo represents a node we discovered that is a member of the staking
/// committee
#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct NodeInfo {
    /// name of the node, can be anything
    pub name: String,
    /// the dns or ip address of the node with port number
    pub network_address: String,
    /// the pubkey stored on chain
    pub network_public_key: Vec<u8>,
}

/// get_node_info will take a committee member object_id and build a NodeInfo
/// for it
async fn get_node_info(url: &str, object_id: &str) -> Result<NodeInfo, Error> {
    let method = "sui_getObject";
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method":method,
        "id":1,
        "params": [object_id, {"showContent": true}]
    });

    let response = reqwest::Client::new()
        .post(url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    if !response.status().is_success() {
        let error_message = response.text().await?;
        return Err(anyhow::anyhow!("query call failed; {error_message}"));
    }
    // Parse the response as JSON
    let response_json: serde_json::Value = response.json().await?;

    // Extract and print the relevant fields
    let Some(fields) =
        response_json["result"]["data"]["content"]["fields"]["node_info"]["fields"].as_object()
    else {
        return Err(anyhow::anyhow!(
            "unable to get committee object id.  method: {method} object_id: {object_id}"
        ));
    };
    let Some(name) = fields["name"].as_str() else {
        return Err(anyhow::anyhow!(
            "unable to get committee object name.  method: {method} object_id: {object_id}"
        ));
    };
    let Some(network_address) = fields["network_address"].as_str() else {
        return Err(anyhow::anyhow!(
            "unable to get committee object network_address.  \
method: {method} object_id: {object_id}"
        ));
    };
    let Some(network_public_key) = fields["network_public_key"].as_array() else {
        return Err(anyhow::anyhow!(
            "unable to get committee object network_public_key.  \
method: {method} object_id: {object_id}"
        ));
    };

    let network_public_key: Vec<u8> = network_public_key
        .iter()
        // somewhat difficult to extract u8
        .filter_map(|v| v.as_u64().map(|num| num as u8))
        .collect();

    Ok(NodeInfo {
        name: name.to_string(),
        network_address: network_address.to_string(),
        network_public_key: network_public_key.to_vec(),
    })
}

/// get_walrus_committee is the entry point for pub callers to get the set of
/// committee members
pub async fn get_walrus_committee(
    url: &str,
    staking_object_id: &str,
) -> Result<Vec<NodeInfo>, Error> {
    let dynamic_field_object_id = get_well_known_staking_object_id(url, staking_object_id).await?;
    let committee_object_ids = get_committee(url, &dynamic_field_object_id).await?;
    let node_info_futures = committee_object_ids
        .iter()
        .map(|object_id| get_node_info(url, object_id));
    let nodes = try_join_all(node_info_futures).await?;
    if nodes.is_empty() {
        return Err(anyhow::anyhow!(
            "unable to get any committee objects; \
the node cache won't be updated until this call succeeds"
        ));
    }
    Ok(nodes)
}

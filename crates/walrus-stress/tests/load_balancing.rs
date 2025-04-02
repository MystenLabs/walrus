use std::{sync::Arc, collections::HashMap};
use anyhow::Result;
use prometheus::Registry;
use walrus_service::client::metrics::ClientMetrics;

struct Node {
    id: u64,
    capacity: usize,
    current_load: usize,
    availability: f64,  // Availability in range 0.0-1.0 (1.0 = 100% available)
}

struct BlobAllocation {
    blob_id: u64,
    size: usize,
    assigned_node: u64,
}


struct LoadBalancingResult {
    allocations: Vec<BlobAllocation>,
    load_variance: f64,  
    utilization_rate: f64,  
}


fn setup_metrics() -> Arc<ClientMetrics> {
    let registry = Registry::new();
    Arc::new(ClientMetrics::new(&registry))
}

fn create_test_cluster(node_count: usize) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(node_count);
    for i in 0..node_count {
        let capacity = 10_000 + (i % 3) * 2_000;
        let availability = 0.95 + (i % 5) as f64 * 0.01;
        
        nodes.push(Node {
            id: i as u64,
            capacity,
            current_load: 0,
            availability,
        });
    }
    nodes
}

fn find_best_node(blob_size: usize, nodes: &mut [Node]) -> Option<u64> {
    let mut best_score = f64::NEG_INFINITY;
    let mut best_node_idx = None;
    
    for (idx, node) in nodes.iter().enumerate() {
        let remaining_capacity = node.capacity.saturating_sub(node.current_load);
        if remaining_capacity >= blob_size {
            let score = node.availability * 0.7 + (remaining_capacity as f64 / node.capacity as f64) * 0.3;
            if score > best_score {
                best_score = score;
                best_node_idx = Some(idx);
            }
        }
    }
    
    best_node_idx.map(|idx| {
        nodes[idx].current_load += blob_size;
        nodes[idx].id
    })
}
// Function to simulate blob allocation
fn allocate_blobs(blob_sizes: &[usize], nodes: &mut [Node]) -> LoadBalancingResult {
    let mut allocations = Vec::with_capacity(blob_sizes.len());
    
    for (i, &size) in blob_sizes.iter().enumerate() {
        if let Some(node_id) = find_best_node(size, nodes) {
            allocations.push(BlobAllocation {
                blob_id: i as u64,
                size,
                assigned_node: node_id,
            });
        }
    }
    
    let load_percentages: Vec<f64> = nodes
        .iter()
        .map(|node| node.current_load as f64 / node.capacity as f64)
        .collect();
    
    
    let mean_load = load_percentages.iter().sum::<f64>() / load_percentages.len() as f64;
    let variance = load_percentages
        .iter()
        .map(|&load| (load - mean_load).powi(2))
        .sum::<f64>() / load_percentages.len() as f64;
    
    let total_used = nodes.iter().map(|node| node.current_load).sum::<usize>();
    let total_capacity = nodes.iter().map(|node| node.capacity).sum::<usize>();
    let utilization = total_used as f64 / total_capacity as f64;
    
    LoadBalancingResult {
        allocations,
        load_variance: variance,
        utilization_rate: utilization,
    }
}

fn record_load_balancing_metrics(metrics: &ClientMetrics, result: &LoadBalancingResult, nodes: &[Node]) {
    for _ in 0..result.allocations.len() {
        metrics.observe_submitted("blob_allocation");
    }
    
    let mut node_allocations = HashMap::new();
    for allocation in &result.allocations {
        *node_allocations.entry(allocation.assigned_node).or_insert(0) += 1;
    }
    
    let avg_allocations = result.allocations.len() as f64 / nodes.len() as f64;
    let imbalanced_nodes = node_allocations
        .values()
        .filter(|&&count| (count as f64 - avg_allocations).abs() > avg_allocations * 0.3)
        .count();
    
    for _ in 0..imbalanced_nodes {
        metrics.observe_error("imbalanced_node");
    }
    
    let failed_allocations = 0;  
        for _ in 0..failed_allocations {
        metrics.observe_error("allocation_failure");
    }
}

#[tokio::test]
async fn test_load_balancing_distribution() -> Result<()> {
    let metrics = setup_metrics();
    
    let mut nodes = create_test_cluster(5);
    
    // Create blobs of various sizes
    let blob_sizes = vec![500, 1000, 750, 1200, 1500, 800, 600, 900, 1100, 1300];
    
    let result = allocate_blobs(&blob_sizes, &mut nodes);
    
    record_load_balancing_metrics(&metrics, &result, &nodes);
    
    assert_eq!(result.allocations.len(), blob_sizes.len(), "Some blobs were not allocated");
    
    assert!(result.load_variance < 0.05, "Load variance between nodes is too high: {}", result.load_variance);
    
    assert_eq!(
        metrics.submitted.get_metric_with_label_values(&["blob_allocation"]).unwrap().get(),
        blob_sizes.len() as f64
    );
    
    Ok(())
}

#[tokio::test]
async fn test_load_balancing_with_hotspots() -> Result<()> {
    let metrics = setup_metrics();
    
    let mut nodes = vec![
        Node { id: 0, capacity: 5_000, current_load: 0, availability: 0.99 },
        Node { id: 1, capacity: 20_000, current_load: 0, availability: 0.98 },
        Node { id: 2, capacity: 8_000, current_load: 0, availability: 0.95 },
        Node { id: 3, capacity: 15_000, current_load: 0, availability: 0.97 },
    ];
    
    // Create blobs of various sizes (some are large)
    let blob_sizes = vec![1000, 800, 7000, 1200, 10000, 2000, 900, 1500, 5000, 3000];
    
    let result = allocate_blobs(&blob_sizes, &mut nodes);
    
    record_load_balancing_metrics(&metrics, &result, &nodes);
    
    let large_blobs = blob_sizes.iter().enumerate()
        .filter(|(_, &size)| size > 5000)
        .map(|(i, _)| i as u64)
        .collect::<Vec<_>>();
    
    for blob_id in large_blobs {
        let allocation = result.allocations.iter().find(|a| a.blob_id == blob_id).unwrap();
        let node = nodes.iter().find(|n| n.id == allocation.assigned_node).unwrap();
        assert!(node.capacity >= 15_000, "Large blob was allocated to a low-capacity node");
    }
    
    assert!(result.utilization_rate > 0.3, "System utilization is too low: {}", result.utilization_rate);
    
    Ok(())
}

#[tokio::test]
async fn test_load_balancing_high_availability() -> Result<()> {
    let metrics = setup_metrics();
    
    let mut nodes = vec![
        Node { id: 0, capacity: 10_000, current_load: 0, availability: 0.99 },
        Node { id: 1, capacity: 10_000, current_load: 0, availability: 0.90 },
        Node { id: 2, capacity: 10_000, current_load: 0, availability: 0.80 },
        Node { id: 3, capacity: 10_000, current_load: 0, availability: 0.70 },
    ];
    
    let blob_sizes = vec![1000; 20];
    
    let result = allocate_blobs(&blob_sizes, &mut nodes);
    
    record_load_balancing_metrics(&metrics, &result, &nodes);
    
    let allocations_per_node = result.allocations.iter()
        .fold(HashMap::new(), |mut acc, alloc| {
            *acc.entry(alloc.assigned_node).or_insert(0) += 1;
            acc
        });
    
    let high_availability_node_id = 0;
    let high_avail_allocations = allocations_per_node.get(&high_availability_node_id).cloned().unwrap_or(0);
    
    for (node_id, allocations) in &allocations_per_node {
        if *node_id != high_availability_node_id {
            assert!(high_avail_allocations >= *allocations, 
                    "Node with lower availability received more allocations than the high-availability node");
        }
    }
    
    Ok(())
} 
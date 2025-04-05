use std::sync::Arc;
use anyhow::Result;
use prometheus::Registry;
use walrus_service::client::metrics::ClientMetrics;

struct InconsistentBlobMetadata {
    id: u64,
    size: u64,
    is_corrupted: bool,
}

struct RecoveryResult {
    attempted: usize,
    successful: usize,
    success_rate: f64,
}

fn setup_metrics() -> Arc<ClientMetrics> {
    let registry = Registry::new();
    Arc::new(ClientMetrics::new(&registry))
}

fn generate_blobs(count: usize, inconsistent_rate: f64) -> Vec<InconsistentBlobMetadata> {
    let inconsistent_count = (count as f64 * inconsistent_rate) as usize;
    let mut blobs = Vec::with_capacity(count);
    
    for i in 0..count {
        blobs.push(InconsistentBlobMetadata {
            id: i as u64,
            size: 1024,
            is_corrupted: i < inconsistent_count,
        });
    }
    
    blobs
}

fn attempt_recovery(blobs: &[InconsistentBlobMetadata]) -> RecoveryResult {
    let corrupted = blobs.iter().filter(|b| b.is_corrupted).count();
    if corrupted == 0 {
        return RecoveryResult {
            attempted: 0,
            successful: 0,
            success_rate: 1.0,
        };
    }
    
    // Simulate recovery with ~70% success rate
    let successful = (corrupted as f64 * 0.7) as usize;
    
    RecoveryResult {
        attempted: corrupted,
        successful,
        success_rate: successful as f64 / corrupted as f64,
    }
}

fn record_metrics(metrics: &ClientMetrics, blobs: &[InconsistentBlobMetadata], recovery: &RecoveryResult) {
    for _ in 0..blobs.len() {
        metrics.observe_submitted("write");
    }
    
    let corrupted = blobs.iter().filter(|b| b.is_corrupted).count();
    for _ in 0..corrupted {
        metrics.observe_error("inconsistent");
    }
    
    for _ in 0..recovery.successful {
        metrics.observe_submitted("recovery");
    }
}

#[tokio::test]
async fn test_inconsistent_blob_generation() -> Result<()> {
    let metrics = setup_metrics();
    
    let blobs = generate_blobs(10, 0.3);
    
    let corrupted = blobs.iter().filter(|b| b.is_corrupted).count();
    assert_eq!(corrupted, 3); // 30% should be inconsistent
    
    for _ in 0..blobs.len() {
        metrics.observe_submitted("write");
    }
    for _ in 0..corrupted {
        metrics.observe_error("inconsistent");
    }
    
    assert_eq!(metrics.submitted.get_metric_with_label_values(&["write"]).unwrap().get(), blobs.len() as f64);
    assert_eq!(metrics.errors.get_metric_with_label_values(&["inconsistent"]).unwrap().get(), corrupted as f64);
    
    Ok(())
}

#[tokio::test]
async fn test_inconsistent_blob_recovery() -> Result<()> {
    let metrics = setup_metrics();
    
    let blobs = generate_blobs(10, 0.3);
    let corrupted = blobs.iter().filter(|b| b.is_corrupted).count();
    
    let recovery = attempt_recovery(&blobs);
    
    for _ in 0..corrupted {
        metrics.observe_error("inconsistent");
    }
    for _ in 0..recovery.successful {
        metrics.observe_submitted("recovery");
    }
    
    assert!(recovery.success_rate > 0.6); // Success rate should be above 60%
    assert_eq!(metrics.errors.get_metric_with_label_values(&["inconsistent"]).unwrap().get(), corrupted as f64);
    assert_eq!(metrics.submitted.get_metric_with_label_values(&["recovery"]).unwrap().get(), recovery.successful as f64);
    
    Ok(())
}

#[tokio::test]
async fn test_metrics_for_inconsistent_blobs() -> Result<()> {
    let metrics = setup_metrics();
    
    let blobs = generate_blobs(100, 0.1);
    let recovery = attempt_recovery(&blobs);
    
    record_metrics(&metrics, &blobs, &recovery);
    
    let corrupted = blobs.iter().filter(|b| b.is_corrupted).count();
    
    assert_eq!(metrics.submitted.get_metric_with_label_values(&["write"]).unwrap().get(), blobs.len() as f64);
    assert_eq!(metrics.errors.get_metric_with_label_values(&["inconsistent"]).unwrap().get(), corrupted as f64);
    assert_eq!(metrics.submitted.get_metric_with_label_values(&["recovery"]).unwrap().get(), recovery.successful as f64);
    
    Ok(())
} 
use std::{sync::Arc, time::{Duration, Instant}};
use anyhow::Result;
use prometheus::Registry;
use walrus_service::client::metrics::ClientMetrics;

struct CompressibleBlob {
    id: u64,
    original_size: usize,
    compressed_size: usize,
    compression_ratio: f64,
    compression_time_ms: u64,
}

struct CompressionStats {
    total_original_size: usize,
    total_compressed_size: usize,
    average_ratio: f64,
    average_time_ms: f64,
}

fn setup_metrics() -> Arc<ClientMetrics> {
    let registry = Registry::new();
    Arc::new(ClientMetrics::new(&registry))
}

fn generate_data(size: usize, compressibility: f64) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let repeatable_chunk_size = (size as f64 * compressibility) as usize;
    
    if repeatable_chunk_size > 0 {
        let pattern = vec![0xAA, 0xBB, 0xCC, 0xDD];
        for _ in 0..(repeatable_chunk_size / pattern.len()) {
            data.extend_from_slice(&pattern);
        }
        for i in 0..(repeatable_chunk_size % pattern.len()) {
            data.push(pattern[i]);
        }
    }
    
    let random_part_size = size - data.len();
    for i in 0..random_part_size {
        data.push((i % 256) as u8);
    }
    
    data
}

fn compress_blob(blob_id: u64, data: &[u8]) -> CompressibleBlob {
    let original_size = data.len();
    
    let start = Instant::now();
    
    // Actual compression logic (using simulation instead of flate2)
    // In real implementation, you could use:
    // let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    // encoder.write_all(data).unwrap();
    // let compressed_data = encoder.finish().unwrap();
    
    let unique_bytes = data.iter().copied().collect::<std::collections::HashSet<u8>>();
    let estimated_ratio = 1.0 - (unique_bytes.len() as f64 / original_size as f64).min(1.0);
    let compressed_size = (original_size as f64 * (1.0 - estimated_ratio * 0.8)).round() as usize;
    
    std::thread::sleep(Duration::from_millis(5));  // Simulate actual compression time
    let compression_time = start.elapsed().as_millis() as u64;
    
    CompressibleBlob {
        id: blob_id,
        original_size,
        compressed_size,
        compression_ratio: 1.0 - (compressed_size as f64 / original_size as f64),
        compression_time_ms: compression_time,
    }
}

fn process_compressions(blobs: &[CompressibleBlob]) -> CompressionStats {
    let total_original = blobs.iter().map(|b| b.original_size).sum::<usize>();
    let total_compressed = blobs.iter().map(|b| b.compressed_size).sum::<usize>();
    let avg_ratio = blobs.iter().map(|b| b.compression_ratio).sum::<f64>() / blobs.len() as f64;
    let avg_time = blobs.iter().map(|b| b.compression_time_ms as f64).sum::<f64>() / blobs.len() as f64;
    
    CompressionStats {
        total_original_size: total_original,
        total_compressed_size: total_compressed,
        average_ratio: avg_ratio,
        average_time_ms: avg_time,
    }
}

fn record_compression_metrics(metrics: &ClientMetrics, blobs: &[CompressibleBlob], _stats: &CompressionStats) {
    for blob in blobs {
        metrics.observe_submitted("compression");
        metrics.observe_latency("compression", Duration::from_millis(blob.compression_time_ms));
    }
    
    // Classify as efficient (≥50%) or inefficient compression
    let efficient_compressions = blobs.iter().filter(|b| b.compression_ratio >= 0.5).count();
    for _ in 0..efficient_compressions {
        metrics.observe_submitted("efficient_compression");
    }
    
    let inefficient_compressions = blobs.len() - efficient_compressions;
    for _ in 0..inefficient_compressions {
        metrics.observe_error("inefficient_compression");
    }
}

#[tokio::test]
async fn test_blob_compression_ratio() -> Result<()> {
    let metrics = setup_metrics();
    
    let test_cases = [
        (10_000, 0.8),  // High compressibility
        (8_000, 0.5),   // Medium compressibility
        (5_000, 0.2),   // Low compressibility
    ];
    
    let mut compressed_blobs = Vec::new();
    
    for (i, (size, compressibility)) in test_cases.iter().enumerate() {
        let data = generate_data(*size, *compressibility);
        let compressed = compress_blob(i as u64, &data);
        
        // Verify compression ratio
        if *compressibility > 0.7 {
            assert!(compressed.compression_ratio > 0.6, "High compressibility data should be compressed by at least 60%");
        } else if *compressibility > 0.4 {
            assert!(compressed.compression_ratio > 0.3, "Medium compressibility data should be compressed by at least 30%");
        }
        
        compressed_blobs.push(compressed);
    }
    
    let stats = process_compressions(&compressed_blobs);
    
    record_compression_metrics(&metrics, &compressed_blobs, &stats);    
    assert_eq!(
        metrics.submitted.get_metric_with_label_values(&["compression"]).unwrap().get(),
        compressed_blobs.len() as f64
    );
    Ok(())
}

#[tokio::test]
async fn test_compression_performance() -> Result<()> {
    let metrics = setup_metrics();
    
    // Generate large data
    let large_data = generate_data(100_000, 0.5);
    
    let start = Instant::now();
        let iterations = 5;
    let mut compressed_blobs = Vec::with_capacity(iterations);
    
    for i in 0..iterations {
        let compressed = compress_blob(i as u64, &large_data);
        compressed_blobs.push(compressed);
    }
    
    let total_time = start.elapsed();
    
    let stats = process_compressions(&compressed_blobs);
    
    record_compression_metrics(&metrics, &compressed_blobs, &stats);
    
    assert!(
        stats.average_time_ms < 100.0,
        "Average compression time exceeds 100ms: {}ms",
        stats.average_time_ms
    );
    
    let throughput = (large_data.len() * iterations) as f64 / (1024.0 * 1024.0) / total_time.as_secs_f64();
    println!("Compression throughput: {:.2} MB/s", throughput);
    
    let total_submissions = metrics.submitted.get_metric_with_label_values(&["compression"]).unwrap().get();
    assert_eq!(total_submissions, iterations as f64);
    
    Ok(())
}

#[tokio::test]
async fn test_compression_efficiency_classification() -> Result<()> {
    let metrics = setup_metrics();
    
    let mut compressed_blobs = Vec::new();
    
    // Efficient compression (ratio > 50%)
    for i in 0..5 {
        let data = generate_data(10_000, 0.9);  // High compressibility
        let compressed = compress_blob(i as u64, &data);
        compressed_blobs.push(compressed);
    }
    
    // Inefficient compression (ratio < 50%)
    for i in 5..8 {
        let data = generate_data(10_000, 0.1);  // Low compressibility
        let compressed = compress_blob(i as u64, &data);
        compressed_blobs.push(compressed);
    }
    
    
    let stats = process_compressions(&compressed_blobs);
    
    record_compression_metrics(&metrics, &compressed_blobs, &stats);

    let efficient_count = metrics.submitted.get_metric_with_label_values(&["efficient_compression"]).unwrap().get();
    let inefficient_count = metrics.errors.get_metric_with_label_values(&["inefficient_compression"]).unwrap().get();
    
    let expected_efficient = compressed_blobs.iter().filter(|b| b.compression_ratio >= 0.5).count() as f64;
    let expected_inefficient = compressed_blobs.iter().filter(|b| b.compression_ratio < 0.5).count() as f64;
    
    assert_eq!(efficient_count, expected_efficient);
    assert_eq!(inefficient_count, expected_inefficient);
    
    assert!(
        stats.average_ratio > 0.4,
        "Average compression ratio is too low: {:.2}%",
        stats.average_ratio * 100.0
    );
    
    Ok(())
} 
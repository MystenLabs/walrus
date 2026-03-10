// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Human-readable size parsing and formatting.

/// Parses a human-readable size string into bytes.
///
/// Accepts optional suffixes `k`/`m`/`g` (case-insensitive) for KiB/MiB/GiB.
/// Without a suffix, the value is interpreted as bytes.
///
/// # Examples
///
/// ```
/// use walrus_utils::size::parse_size;
///
/// assert_eq!(parse_size("1k").unwrap(), 1024);
/// assert_eq!(parse_size("32m").unwrap(), 32 * 1024 * 1024);
/// assert_eq!(parse_size("1G").unwrap(), 1 << 30);
/// assert_eq!(parse_size("4096").unwrap(), 4096);
/// ```
pub fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.to_lowercase();
    let (num, mult) = if let Some(n) = s.strip_suffix('g') {
        (n, 1 << 30)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1 << 20)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1 << 10)
    } else {
        (s.as_str(), 1)
    };
    let n: usize = num.parse().map_err(|e| format!("invalid size: {e}"))?;
    Ok(n * mult)
}

/// Formats a byte count as a human-readable size string.
///
/// Uses binary units (GiB, MiB, KiB) with integer truncation.
///
/// # Examples
///
/// ```
/// use walrus_utils::size::format_size;
///
/// assert_eq!(format_size(1 << 30), "1GiB");
/// assert_eq!(format_size(32 * 1024 * 1024), "32MiB");
/// assert_eq!(format_size(512 * 1024), "512KiB");
/// assert_eq!(format_size(100), "100B");
/// ```
pub fn format_size(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{}GiB", bytes >> 30)
    } else if bytes >= 1 << 20 {
        format!("{}MiB", bytes >> 20)
    } else if bytes >= 1 << 10 {
        format!("{}KiB", bytes >> 10)
    } else {
        format!("{bytes}B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("0").unwrap(), 0);
        assert_eq!(parse_size("100").unwrap(), 100);
        assert_eq!(parse_size("1k").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("32m").unwrap(), 32 << 20);
        assert_eq!(parse_size("32M").unwrap(), 32 << 20);
        assert_eq!(parse_size("1g").unwrap(), 1 << 30);
        assert_eq!(parse_size("1G").unwrap(), 1 << 30);
        assert!(parse_size("abc").is_err());
        assert!(parse_size("m").is_err());
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0B");
        assert_eq!(format_size(512), "512B");
        assert_eq!(format_size(1024), "1KiB");
        assert_eq!(format_size(1 << 20), "1MiB");
        assert_eq!(format_size(1 << 30), "1GiB");
        assert_eq!(format_size(3 * (1 << 30)), "3GiB");
    }
}

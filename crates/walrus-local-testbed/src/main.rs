//! The local testbed runner.
use std::{
    fs,
    path::{Path, PathBuf},
    process::{exit, Command},
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use glob::glob;
use nix::sys::signal::{self, SigHandler, Signal};
use serde_yaml::{self, Value};
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt,
    prelude::*,
};

#[derive(Parser, Debug)]
#[clap(name = "walrus-runner")]
struct Args {
    /// Tail the logs of the spawned nodes.
    #[clap(short, long, default_value = "false")]
    tail_logs: bool,

    #[clap(short, long, default_value = "4")]
    committee_size: u32,

    #[clap(short, long, default_value = "10")]
    shards: u32,

    #[clap(short, long, default_value = "devnet")]
    network: String,

    #[clap(short, long, default_value = "1h")]
    duration: String,

    #[clap(short, long)]
    existing: bool,

    #[clap(long)]
    clean: bool,
}

fn kill_tmux_sessions() -> Result<()> {
    let output = Command::new("tmux")
        .arg("ls")
        .output()
        .expect("tmux ls failed");

    if output.status.success() {
        let sessions = String::from_utf8_lossy(&output.stdout);
        for line in sessions.lines() {
            if line.contains("dryrun-node-") {
                let session_name = line.split(':').next().unwrap();
                Command::new("tmux")
                    .args(["kill-session", "-t", session_name])
                    .output()?;
            }
        }
    } else {
        tracing::error!(
            "tmux ls failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn run_node(working_dir: &Path, config_name: &str, cleanup_storage: bool) -> Result<()> {
    let args: &[String] = &[
        "new".into(),
        "-d".into(),
        "-s".into(),
        config_name.into(),
        shlex::try_quote(&format!(
            "./target/release/walrus-node run --config-path {}/{}.yaml{}",
            working_dir.display(),
            config_name,
            if cleanup_storage {
                " --cleanup-storage"
            } else {
                ""
            }
        ))?
        .to_string(),
    ];
    tracing::info!("Starting node: {}", config_name);
    let output = Command::new("tmux")
        .args(args)
        .output()
        .context("Failed to start tmux session")?;
    tracing::info!("tmux output: {}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

fn update_configs(working_dir: &Path, network: &str) -> Result<()> {
    // Read client config
    let client_config_path = working_dir.join("client_config.yaml");
    let client_config: Value = serde_yaml::from_reader(fs::File::open(&client_config_path)?)?;

    // Create backup config
    let backup_config = serde_yaml::mapping::Mapping::from_iter([
        (
            "backup_storage_path".into(),
            format!("{}/backup-db", working_dir.display()).into(),
        ),
        (
            "sui".into(),
            serde_yaml::mapping::Mapping::from_iter([
                (
                    "rpc".into(),
                    format!("https://fullnode.{network}.sui.io:443").into(),
                ),
                (
                    "system_object".into(),
                    client_config["system_object"].clone(),
                ),
                (
                    "staking_object".into(),
                    client_config["staking_object"].clone(),
                ),
            ])
            .into(),
        ),
    ]);

    tracing::info!("Writing backup config: {:#?}", backup_config);
    // Write backup config
    serde_yaml::to_writer(
        fs::File::create(working_dir.join("backup_config.yaml"))?,
        &backup_config,
    )?;

    // Update node configs to include adaptive downloader config.
    for entry in glob(&format!("{}/dryrun-node-*.yaml", working_dir.display()))? {
        let path = entry?;
        if path
            .file_name()
            .is_none_or(|filename| filename.to_str().is_none_or(|s| s.contains("-sui")))
        {
            continue;
        }
        let mut config: Value = serde_yaml::from_reader(fs::File::open(&path)?)?;

        let event_processor_config = serde_yaml::mapping::Mapping::from_iter([(
            "adaptive_downloader_config".into(),
            serde_yaml::mapping::Mapping::from_iter([
                ("max_workers".into(), 2.into()),
                ("initial_workers".into(), 2.into()),
            ])
            .into(),
        )]);

        config.as_mapping_mut().unwrap().insert(
            "event_processor_config".into(),
            event_processor_config.into(),
        );

        serde_yaml::to_writer(fs::File::create(&path)?, &config)?;
    }

    Ok(())
}

extern "C" fn handle_sigint(signal: libc::c_int) {
    let signal = Signal::try_from(signal).unwrap();
    tracing::error!("Received signal: {:?}", signal);
    if signal == Signal::SIGINT {
        // Kill existing sessions
        kill_tmux_sessions().expect("kill_tmux_sessions failed");
        std::process::exit(0);
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    tracing::info!("Starting local-testbed with args: {:?}", args);
    if args.committee_size == 0 || args.shards < args.committee_size {
        tracing::error!("Invalid committee size or shard count");
        exit(1);
    }

    // Set up Ctrl+C handler
    let handler = SigHandler::Handler(handle_sigint);
    unsafe { signal::signal(Signal::SIGINT, handler) }.unwrap();

    // Kill existing sessions
    kill_tmux_sessions()?;

    tracing::info!("Building walrus binaries...");
    Command::new("cargo")
        .args([
            "build",
            "--release",
            "--bin",
            "walrus",
            "--bin",
            "walrus-node",
            "--bin",
            "walrus-deploy",
            "--features",
            "deploy",
        ])
        .status()?;

    let working_dir = PathBuf::from("./working_dir");
    let ips = vec!["127.0.0.1".to_string(); args.committee_size as usize];
    let cleanup = !args.existing;

    if !args.existing {
        // Clean up old configs
        for entry in glob(&format!("{}/dryrun-node-*yaml", working_dir.display()))? {
            fs::remove_file(entry?)?;
        }

        // Deploy system contract
        Command::new("./target/release/walrus-deploy")
            .args([
                "deploy-system-contract",
                "--working-dir",
                working_dir.to_str().unwrap(),
                "--sui-network",
                &args.network,
                "--n-shards",
                &args.shards.to_string(),
                "--storage-price",
                "5",
                "--write-price",
                "1",
                "--epoch-duration",
                &args.duration,
                "--host-addresses",
            ])
            .args(ips)
            .status()?;

        // Generate configs
        Command::new("./target/release/walrus-deploy")
            .args([
                "generate-dry-run-configs",
                "--working-dir",
                working_dir.to_str().unwrap(),
            ])
            .status()?;

        update_configs(&working_dir, &args.network)?;
    }

    // Start nodes
    let mut node_count = 0;
    for entry in glob(&format!(
        "{}/dryrun-node-[0-9]*.yaml",
        working_dir.display()
    ))? {
        let path = entry?;
        let node_name = path.file_stem().unwrap().to_str().unwrap();
        run_node(&working_dir, node_name, cleanup)?;
        node_count += 1;
    }

    tracing::info!("\nSpawned {node_count} nodes in separate tmux sessions.");
    tracing::info!(
        "Client configuration stored at '{}/client_config.yaml'",
        working_dir.display()
    );

    if args.tail_logs {
        Command::new("tail")
            .args(["-F", &format!("{}/dryrun-node-*log", working_dir.display())])
            .status()?;
    } else {
        tracing::info!("Press Ctrl+C to stop the nodes.");
        loop {
            thread::sleep(Duration::from_secs(120));
        }
    }

    Ok(())
}

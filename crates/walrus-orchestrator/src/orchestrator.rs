// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::PathBuf,
};

use tokio::time::{self, Instant};
use walrus_core::ensure;

use crate::{
    benchmark::BenchmarkParameters,
    client::Instance,
    display,
    error::{TestbedError, TestbedResult},
    logs::LogsAnalyzer,
    measurements::{Measurement, MeasurementsCollection},
    monitor::Monitor,
    protocol::{ProtocolCommands, ProtocolMetrics},
    settings::Settings,
    ssh::{CommandContext, CommandStatus, SshConnectionManager},
};

/// An orchestrator to deploy nodes and run benchmarks on a testbed.
pub struct Orchestrator<P> {
    /// The testbed's settings.
    settings: Settings,
    /// The state of the testbed (reflecting accurately the state of the machines).
    instances: Vec<Instance>,
    /// Provider-specific commands to install on the instance.
    instance_setup_commands: Vec<String>,
    /// Protocol-specific commands generator to generate the protocol configuration files,
    /// boot clients and nodes, etc.
    protocol_commands: P,
    /// Handle ssh connections to instances.
    ssh_manager: SshConnectionManager,
    /// Skip the testbed update. Setting this value to true is dangerous and may lead to
    /// unexpected behavior.
    skip_testbed_update: bool,
    /// Skip the testbed configuration. Setting this value to true is dangerous and may
    /// lead to unexpected behavior.
    skip_testbed_configuration: bool,
}

impl<P> Orchestrator<P> {
    /// Make a new orchestrator.
    pub fn new(
        settings: Settings,
        instances: Vec<Instance>,
        instance_setup_commands: Vec<String>,
        protocol_commands: P,
        ssh_manager: SshConnectionManager,
    ) -> Self {
        Self {
            settings,
            instances,
            instance_setup_commands,
            protocol_commands,
            ssh_manager,
            skip_testbed_update: false,
            skip_testbed_configuration: false,
        }
    }

    /// Skip the testbed update.
    pub fn skip_testbed_update(mut self, skip_testbed_update: bool) -> Self {
        if skip_testbed_update {
            display::warn("Skipping testbed update! Use with care!");
            self.settings.repository.set_unknown_commit();
        }
        self.skip_testbed_update = skip_testbed_update;
        self
    }

    /// Skip the testbed configuration.
    pub fn skip_testbed_configuration(mut self, skip_testbed_configuration: bool) -> Self {
        if skip_testbed_configuration {
            display::warn("Skipping testbed configuration! Use with care!");
        }
        self.skip_testbed_configuration = skip_testbed_configuration;
        self
    }

    /// Returns the instances of the testbed on which to run the benchmarks.
    ///
    /// This function returns two vectors of instances; the first contains the instances on which to
    /// run the load generators and the second contains the instances on which to run the nodes.
    /// Additionally returns an optional monitoring instance.
    pub fn select_instances(
        &self,
        parameters: &BenchmarkParameters,
    ) -> TestbedResult<(Vec<Instance>, Option<Instance>)> {
        // Ensure there are enough active instances.
        let available_instances: Vec<_> = self.instances.iter().filter(|x| x.is_active()).collect();
        let minimum_instances = parameters.clients + if self.settings.monitoring { 1 } else { 0 };
        ensure!(
            available_instances.len() >= minimum_instances,
            TestbedError::InsufficientCapacity(minimum_instances - available_instances.len())
        );

        // Sort the instances by region. This step ensures that the instances are selected as
        // equally as possible from all regions.
        let mut instances_by_regions = HashMap::new();
        for instance in available_instances {
            instances_by_regions
                .entry(&instance.region)
                .or_insert_with(VecDeque::new)
                .push_back(instance);
        }

        // Select the instance to host the monitoring stack. We select this instance from the region
        // with the most instances.
        let mut monitoring_instance = None;
        if self.settings.monitoring {
            monitoring_instance = instances_by_regions
                .values_mut()
                .max_by_key(|instances| instances.len())
                .map(|instances| instances.pop_front().unwrap().clone());
        }

        // Select the instances to host exclusively load generators.
        let mut client_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if client_instances.len() == parameters.clients {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region) {
                if let Some(instance) = regional_instances.pop_front() {
                    client_instances.push(instance.clone());
                }
            }
        }

        Ok((client_instances, monitoring_instance))
    }
}

impl<P: ProtocolCommands + ProtocolMetrics> Orchestrator<P> {
    /// Install the codebase and its dependencies on the testbed.
    pub async fn install(&self) -> TestbedResult<()> {
        display::action("Installing dependencies on all machines");

        let working_dir = self.settings.working_dir.display();
        let url = &self.settings.repository.url;
        let basic_commands = [
            "sudo apt-get update",
            "sudo apt-get -y upgrade",
            "sudo apt-get -y autoremove",
            // Disable "pending kernel upgrade" message.
            "sudo apt-get -y remove needrestart",
            // The following dependencies
            // * build-essential: prevent the error: [error: linker `cc` not found].
            // * sysstat - for getting disk stats
            // * iftop - for getting network stats
            // * libssl-dev - Required to compile the orchestrator
            // TODO(alberto): Remove libssl-dev dependency (#221).
            "sudo apt-get -y install build-essential sysstat iftop libssl-dev",
            "sudo apt-get -y install linux-tools-common linux-tools-generic pkg-config",
            // Install rust (non-interactive).
            "curl --proto \"=https\" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
            "echo \"source $HOME/.cargo/env\" | tee -a ~/.bashrc",
            "source $HOME/.cargo/env",
            "rustup default stable",
            // Create the working directory.
            &format!("mkdir -p {working_dir}"),
            // Clone the repo.
            &format!("(git clone {url} || true)"),
        ];

        let command = [
            &basic_commands[..],
            &Monitor::dependencies()
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<_>>()[..],
            &self
                .instance_setup_commands
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<_>>()[..],
            &self.protocol_commands.protocol_dependencies()[..],
        ]
        .concat()
        .join(" && ");

        let active = self.instances.iter().filter(|x| x.is_active()).cloned();
        let context = CommandContext::default();
        self.ssh_manager.execute(active, command, context).await?;

        display::done();
        Ok(())
    }

    /// Update all instances to use the version of the codebase specified in the setting file.
    pub async fn update(&self) -> TestbedResult<()> {
        display::action("Updating all instances");

        // Update all active instances. This requires compiling the codebase in release (which
        // may take a long time) so we run the command in the background to avoid keeping alive
        // many ssh connections for too long.
        let commit = &self.settings.repository.commit;
        let command = [
            &format!("git fetch origin {commit}"),
            &format!("(git checkout -b {commit} || git checkout -f origin/{commit})"),
            "source $HOME/.cargo/env",
            "RUSTFLAGS=-Ctarget-cpu=native cargo build --release",
        ]
        .join(" && ");

        let active = self.instances.iter().filter(|x| x.is_active()).cloned();

        let id = "update";
        let repo_name = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background(id.into())
            .with_execute_from_path(repo_name.into());
        self.ssh_manager
            .execute(active.clone(), command, context)
            .await?;

        // Wait until the command finished running.
        self.ssh_manager
            .wait_for_command(active, id, CommandStatus::Terminated)
            .await?;

        display::done();
        Ok(())
    }

    /// Configure the instances with the appropriate configuration files.
    pub async fn configure(&self, parameters: &BenchmarkParameters) -> TestbedResult<()> {
        display::action("\nConfiguring instances");

        // Select instances to configure.
        let (instances, _) = self.select_instances(parameters)?;

        // Generate the genesis configuration file and the keystore allowing access to gas objects.
        let command = self
            .protocol_commands
            .genesis_command(instances.iter(), parameters)
            .await;

        let id = "configure";
        let repo_name = self.settings.repository_name();
        let context = CommandContext::new()
            // .run_background(id.into())
            .with_log_file(format!("~/{id}.log").into())
            .with_execute_from_path(repo_name.into());

        self.ssh_manager
            .execute(instances.clone(), command, context)
            .await?;
        self.ssh_manager
            .wait_for_command(instances, id, CommandStatus::Terminated)
            .await?;

        display::done();
        Ok(())
    }

    /// Cleanup all instances and optionally delete their log files.
    pub async fn cleanup(&self, delete_logs: bool) -> TestbedResult<()> {
        display::action("Cleaning up testbed");

        // Kill all tmux servers and delete the nodes dbs. Optionally clear logs.
        let mut command = vec!["(tmux kill-server || true)".into()];
        for path in self.protocol_commands.db_directories() {
            command.push(format!("(rm -rf {} || true)", path.display()));
        }
        if delete_logs {
            command.push("(rm -rf ~/*log* || true)".into());
        }
        let command = command.join(" ; ");

        // Execute the deletion on all machines.
        let active = self.instances.iter().filter(|x| x.is_active()).cloned();
        let context = CommandContext::default();
        self.ssh_manager.execute(active, command, context).await?;

        display::done();
        Ok(())
    }

    /// Reload prometheus and grafana.
    pub async fn start_monitoring(&self, parameters: &BenchmarkParameters) -> TestbedResult<()> {
        let (clients, instance) = self.select_instances(parameters)?;
        if let Some(instance) = instance {
            display::action("Configuring monitoring instance");

            let monitor = Monitor::new(instance, clients, self.ssh_manager.clone());
            monitor
                .start_prometheus(&self.protocol_commands, parameters)
                .await?;
            monitor.start_grafana().await?;

            display::done();
            display::config("Grafana address", monitor.grafana_address());
            display::newline();
        }
        Ok(())
    }

    /// Deploy the load generators.
    pub async fn run_clients(&self, parameters: &BenchmarkParameters) -> TestbedResult<()> {
        display::action("Setting up load generators");

        // Select the instances to run.
        let (clients, _) = self.select_instances(parameters)?;

        for client in &clients {
            display::config("Client address", client.main_ip);
        }

        // Deploy the load generators.
        let targets = self
            .protocol_commands
            .client_command(clients.clone(), parameters);

        let repo = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background("client".into())
            .with_log_file("~/client.log".into())
            .with_execute_from_path(repo.into());
        self.ssh_manager
            .execute_per_instance(targets, context)
            .await?;

        // Wait until all load generators are reachable.
        let commands = self
            .protocol_commands
            .clients_metrics_command(clients, parameters);
        self.ssh_manager.wait_for_success(commands).await;

        display::done();
        Ok(())
    }

    /// Collect metrics from the load generators.
    pub async fn run(
        &self,
        parameters: &BenchmarkParameters,
    ) -> TestbedResult<MeasurementsCollection> {
        display::action(format!(
            "Scraping metrics (at least {}s)",
            self.settings.benchmark_duration.as_secs()
        ));

        // Select the instances to run.
        let (clients, _) = self.select_instances(parameters)?;

        // Regularly scrape the client metrics.
        let metrics_commands = self
            .protocol_commands
            .clients_metrics_command(clients, parameters);

        let mut aggregator = MeasurementsCollection::new(parameters.clone());
        let mut metrics_interval = time::interval(self.settings.scrape_interval);
        metrics_interval.tick().await; // The first tick returns immediately.

        let start = Instant::now();
        loop {
            tokio::select! {
                // Scrape metrics.
                now = metrics_interval.tick() => {
                    let elapsed = now.duration_since(start).as_secs_f64().ceil() as u64;
                    display::status(format!("{elapsed}s"));

                    let stdio = self
                        .ssh_manager
                        .execute_per_instance(metrics_commands.clone(), CommandContext::default())
                        .await?;
                    for (i, (stdout, _stderr)) in stdio.iter().enumerate() {
                        for (label, measurement) in Measurement::from_prometheus::<P>(stdout) {
                            aggregator.add(i, label,measurement);
                        }
                    }

                    let results_directory = &self.settings.results_dir;
                    let commit = &self.settings.repository.commit;
                    let path: PathBuf = results_directory.join(format!("results-{commit}"));
                    fs::create_dir_all(&path).expect("Failed to create log directory");
                    aggregator.save(path);

                    let benchmark_duration = parameters.settings.benchmark_duration.as_secs();
                    if elapsed > benchmark_duration {
                        break;
                    }
                }
            }
        }

        display::done();
        Ok(aggregator)
    }

    /// Download the log files from the nodes and clients.
    pub async fn download_logs(
        &self,
        parameters: &BenchmarkParameters,
    ) -> TestbedResult<LogsAnalyzer> {
        // Select the instances to run.
        let (clients, _) = self.select_instances(parameters)?;

        // Create a log sub-directory for this run.
        let commit = &self.settings.repository.commit;
        let path: PathBuf = [
            &self.settings.logs_dir,
            &format!("logs-{commit}").into(),
            &format!("logs-{parameters:?}").into(),
        ]
        .iter()
        .collect();
        fs::create_dir_all(&path).expect("Failed to create log directory");

        // NOTE: Our ssh library does not seem to be able to transfers files in parallel reliably.
        let mut log_parsers = Vec::new();

        // Download the clients log files.
        display::action("Downloading clients logs");
        for (i, instance) in clients.iter().enumerate() {
            display::status(format!("{}/{}", i + 1, clients.len()));

            let connection = self.ssh_manager.connect(instance.ssh_address()).await?;
            let client_log_content = connection.download("client.log")?;

            let client_log_file = [path.clone(), format!("client-{i}.log").into()]
                .iter()
                .collect::<PathBuf>();
            fs::write(&client_log_file, client_log_content.as_bytes())
                .expect("Cannot write log file");

            let mut log_parser = LogsAnalyzer::default();
            log_parser.set_client_errors(&client_log_content);
            log_parsers.push(log_parser)
        }
        display::done();

        Ok(log_parsers
            .into_iter()
            .max()
            .expect("At least one log parser"))
    }

    /// Run all the benchmarks specified by the benchmark generator.
    pub async fn run_benchmarks(&mut self, parameters: BenchmarkParameters) -> TestbedResult<()> {
        display::header("Preparing testbed");
        display::config("Commit", format!("'{}'", &self.settings.repository.commit));
        display::newline();

        // Cleanup the testbed (in case the previous run was not completed).
        self.cleanup(true).await?;

        // Update the software on all instances.
        if !self.skip_testbed_update {
            self.install().await?;
            self.update().await?;
        }

        // Run all benchmarks.
        display::header("Starting benchmark");
        display::config("Benchmark Parameters", &parameters);
        display::newline();

        // Cleanup the testbed (in case the previous run was not completed).
        self.cleanup(true).await?;
        // Start the instance monitoring tools.
        self.start_monitoring(&parameters).await?;

        // Configure all instances (if needed).
        if !self.skip_testbed_configuration {
            self.configure(&parameters).await?;
        }

        // Deploy the load generators.
        self.run_clients(&parameters).await?;
        if parameters.settings.benchmark_duration.as_secs() == 0 {
            return Ok(());
        }

        // Wait for the benchmark to terminate. Then save the results and print a summary.
        let aggregator = self.run(&parameters).await?;
        aggregator.display_summary();

        // Kill the clients (without deleting the log files).
        self.cleanup(false).await?;

        // Download the log files.
        if self.settings.log_processing {
            let error_counter = self.download_logs(&parameters).await?;
            error_counter.print_summary();
        }

        display::header("Benchmark completed");
        Ok(())
    }
}

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
};

use crate::{
    benchmark::BenchmarkParameters,
    client::Instance,
    display,
    ensure,
    error::{TestbedError, TestbedResult},
    monitor::Monitor,
    protocol::{ProtocolCommands, ProtocolMetrics},
    settings::Settings,
    ssh::{CommandContext, CommandStatus, SshConnectionManager},
};

/// An orchestrator to deploy nodes and run benchmarks on a testbed.
#[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222)
pub struct Orchestrator<P, N, C> {
    /// The testbed's settings.
    settings: Settings,
    /// The state of the testbed (reflecting accurately the state of the machines).
    instances: Vec<Instance>,
    /// The node's configuration parameters.
    node_config: PhantomData<N>,
    /// The client's configuration parameters.
    client_config: PhantomData<C>,
    /// Provider-specific commands to install on the instance.
    instance_setup_commands: Vec<String>,
    /// Protocol-specific commands generator to generate the protocol configuration files,
    /// boot clients and nodes, etc.
    protocol_commands: P,
    /// Handle ssh connections to instances.
    ssh_manager: SshConnectionManager,
}

#[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222)
impl<P, N, C> Orchestrator<P, N, C> {
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
            node_config: PhantomData,
            client_config: PhantomData,
            instance_setup_commands,
            protocol_commands,
            ssh_manager,
        }
    }

    /// Returns the instances of the testbed on which to run the benchmarks. 
    /// 
    /// This function returns two vectors of instances; the first contains the instances on which to
    /// run the load generators and the second contains the instances on which to run the nodes.
    /// Additionally returns an optional monitoring instance.
    pub fn select_instances(
        &self,
        parameters: &BenchmarkParameters,
    ) -> TestbedResult<(Vec<Instance>, Vec<Instance>, Option<Instance>)> {
        // Ensure there are enough active instances.
        let available_instances: Vec<_> = self.instances.iter().filter(|x| x.is_active()).collect();
        let minimum_instances = if self.settings.monitoring {
            parameters.nodes + self.settings.dedicated_clients + 1
        } else {
            parameters.nodes + self.settings.dedicated_clients
        };
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

        // Select the instance to host the monitoring stack.
        let mut monitoring_instance = None;
        if self.settings.monitoring {
            for region in &self.settings.regions {
                if let Some(regional_instances) = instances_by_regions.get_mut(region) {
                    if let Some(instance) = regional_instances.pop_front() {
                        monitoring_instance = Some(instance.clone());
                    }
                    break;
                }
            }
        }

        // Select the instances to host exclusively load generators.
        let mut client_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if client_instances.len() == self.settings.dedicated_clients {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region) {
                if let Some(instance) = regional_instances.pop_front() {
                    client_instances.push(instance.clone());
                }
            }
        }

        // Select the instances to host the nodes.
        let mut nodes_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if nodes_instances.len() == parameters.nodes {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region) {
                if let Some(instance) = regional_instances.pop_front() {
                    nodes_instances.push(instance.clone());
                }
            }
        }

        // Spawn a load generate collocated with each node if there are no instances dedicated
        // to excursively run load generators.
        if client_instances.is_empty() {
            client_instances = nodes_instances.clone();
        }

        Ok((client_instances, nodes_instances, monitoring_instance))
    }
}

#[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222)
impl<P: ProtocolCommands<N, C> + ProtocolMetrics, N, C> Orchestrator<P, N, C> {
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
            // * libssl-dev - Required to compile the orchestrator, todo remove this dependency
            "sudo apt-get -y install build-essential sysstat iftop libssl-dev",
            // * linux-tools-common linux-tools-generic linux-tools-* - installs perf
            // Perf is optional as sometimes AWS releases new kernels without publishing a new
            // linux-tools package (we do not want to fail the deployment when this happens).
            "sudo apt-get -y install linux-tools-common linux-tools-generic linux-tools-`uname -r`
                || echo 'Failed to install perf(optional)'",
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
            &format!("(git checkout -b {commit} {commit} || git checkout origin/{commit})"),
            "source $HOME/.cargo/env",
            "cargo build --release",
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
        display::action("Configuring instances");

        // Select instances to configure.
        let (clients, nodes, _) = self.select_instances(parameters)?;

        // Generate the genesis configuration file and the keystore allowing access to gas objects.
        let command = self
            .protocol_commands
            .genesis_command(nodes.iter(), parameters);
        let repo_name = self.settings.repository_name();
        let context = CommandContext::new().with_execute_from_path(repo_name.into());
        let all = clients.into_iter().chain(nodes);
        self.ssh_manager.execute(all, command, context).await?;

        display::done();
        Ok(())
    }

    /// Cleanup all instances and optionally delete their log files.
    pub async fn cleanup(&self, cleanup: bool) -> TestbedResult<()> {
        display::action("Cleaning up testbed");

        // Kill all tmux servers and delete the nodes dbs. Optionally clear logs.
        let mut command = vec!["(tmux kill-server || true)".into()];
        command.extend(self.protocol_commands.cleanup_commands());
        for path in self.protocol_commands.db_directories() {
            command.push(format!("(rm -rf {} || true)", path.display()));
        }
        if cleanup {
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
        let (clients, nodes, instance) = self.select_instances(parameters)?;
        if let Some(instance) = instance {
            display::action("Configuring monitoring instance");

            let monitor = Monitor::new(
                instance,
                clients,
                nodes,
                self.ssh_manager.clone(),
                self.settings.dedicated_clients != 0,
            );
            monitor.start_prometheus(&self.protocol_commands).await?;
            monitor.start_grafana().await?;

            display::done();
            display::config("Grafana address", monitor.grafana_address());
            display::newline();
        }
        Ok(())
    }

    /// Boot a node on the specified instances.
    async fn boot_nodes(
        &self,
        instances: Vec<Instance>,
        parameters: &BenchmarkParameters,
    ) -> TestbedResult<()> {
        // Run one node per instance.
        let targets = self
            .protocol_commands
            .node_command(instances.clone(), parameters);

        let repo = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background("node".into())
            .with_log_file("~/node.log".into())
            .with_execute_from_path(repo.into());
        self.ssh_manager
            .execute_per_instance(targets, context)
            .await?;

        // Wait until all nodes are reachable.
        let commands = self
            .protocol_commands
            .nodes_metrics_command(instances.clone());
        self.ssh_manager.wait_for_success(commands).await;

        Ok(())
    }

    /// Deploy the nodes.
    pub async fn run_nodes(&self, parameters: &BenchmarkParameters) -> TestbedResult<()> {
        display::action("Deploying validators");

        // Select the instances to run.
        let (_, nodes, _) = self.select_instances(parameters)?;

        // Boot one node per instance.
        self.boot_nodes(nodes, parameters).await?;

        display::done();
        Ok(())
    }

    /// Deploy the load generators.
    pub async fn run_clients(&self, parameters: &BenchmarkParameters) -> TestbedResult<()> {
        display::action("Setting up load generators");

        // Select the instances to run.
        let (clients, _, _) = self.select_instances(parameters)?;

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
        let commands = self.protocol_commands.clients_metrics_command(clients);
        self.ssh_manager.wait_for_success(commands).await;

        display::done();
        Ok(())
    }
}

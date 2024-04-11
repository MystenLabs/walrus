// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, net::SocketAddr, path::PathBuf};

use crate::{
    client::Instance,
    error::{MonitorError, MonitorResult},
    protocol::ProtocolMetrics,
    ssh::{CommandContext, SshConnectionManager},
};

#[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222")
pub struct Monitor {
    instance: Instance,
    clients: Vec<Instance>,
    nodes: Vec<Instance>,
    ssh_manager: SshConnectionManager,
    dedicated_clients: bool,
}

#[allow(dead_code)] // TODO(Alberto): Will be used to deploy nodes (#222")
impl Monitor {
    /// Create a new monitor.
    pub fn new(
        instance: Instance,
        clients: Vec<Instance>,
        nodes: Vec<Instance>,
        ssh_manager: SshConnectionManager,
        dedicated_clients: bool,
    ) -> Self {
        Self {
            instance,
            clients,
            nodes,
            ssh_manager,
            dedicated_clients,
        }
    }

    /// Dependencies to install.
    pub fn dependencies() -> Vec<String> {
        let mut commands = Vec::new();
        commands.extend(Prometheus::install_commands());
        commands.extend(Grafana::install_commands());
        commands.extend(NodeExporter::install_commands());
        commands
    }

    /// Start a prometheus instance on each remote machine.
    pub async fn start_prometheus<P: ProtocolMetrics>(
        &self,
        protocol_commands: &P,
    ) -> MonitorResult<()> {
        // Select the instances to monitor.
        let mut instances = self.nodes.clone();
        if self.dedicated_clients {
            instances.extend(self.clients.iter().cloned())
        }

        // Configure and reload prometheus.
        let instance = [self.instance.clone()];
        let commands = Prometheus::setup_commands(instances, protocol_commands);
        self.ssh_manager
            .execute(instance, commands, CommandContext::default())
            .await?;

        Ok(())
    }

    /// Start grafana on the local host.
    pub async fn start_grafana(&self) -> MonitorResult<()> {
        // Configure and reload grafana.
        let instance = std::iter::once(self.instance.clone());
        let commands = Grafana::setup_commands().join("\n");
        self.ssh_manager
            .execute(instance, commands, CommandContext::default())
            .await?;

        Ok(())
    }

    /// The public address of the grafana instance.
    pub fn grafana_address(&self) -> String {
        format!("http://{}:{}", self.instance.main_ip, Grafana::DEFAULT_PORT)
    }
}

/// Generate the commands to setup prometheus on the given instances.
pub struct Prometheus;

impl Prometheus {
    /// The default prometheus configuration path.
    const DEFAULT_PROMETHEUS_CONFIG_PATH: &'static str = "/etc/prometheus/prometheus.yml";
    /// The default prometheus port.
    pub const DEFAULT_PORT: u16 = 9090;

    /// The commands to install prometheus.
    pub fn install_commands() -> Vec<String> {
        vec![
            "sudo apt-get -y install prometheus".into(),
            "sudo chmod 777 -R /var/lib/prometheus/ /etc/prometheus/".into(),
        ]
    }

    /// Generate the commands to update the prometheus configuration and restart prometheus.
    pub fn setup_commands<I, P>(instances: I, protocol: &P) -> String
    where
        I: IntoIterator<Item = Instance>,
        P: ProtocolMetrics,
    {
        // Generate the prometheus configuration.
        let mut config = vec![Self::global_configuration()];

        let nodes_metrics_path = protocol.nodes_metrics_path(instances);
        for (i, (_, nodes_metrics_path)) in nodes_metrics_path.into_iter().enumerate() {
            let scrape_config = Self::scrape_configuration(i, &nodes_metrics_path);
            config.push(scrape_config);
        }

        // Make the command to configure and restart prometheus.
        format!(
            "sudo echo \"{}\" > {} && sudo service prometheus restart",
            config.join("\n"),
            Self::DEFAULT_PROMETHEUS_CONFIG_PATH
        )
    }

    /// Generate the global prometheus configuration.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn global_configuration() -> String {
        [
            "global:",
            "  scrape_interval: 5s",
            "  evaluation_interval: 5s",
            "scrape_configs:",
        ]
        .join("\n")
    }

    /// Generate the prometheus configuration from the given metrics path.
    /// NOTE: The configuration file is a yaml file so spaces are important.
    fn scrape_configuration(index: usize, nodes_metrics_path: &str) -> String {
        let parts: Vec<_> = nodes_metrics_path.split('/').collect();
        let address = parts[0].parse::<SocketAddr>().unwrap();
        let ip = address.ip();
        let port = address.port();
        let path = parts[1];

        [
            &format!("  - job_name: instance-{index}"),
            &format!("    metrics_path: /{path}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - {ip}:{port}"),
            &format!("  - job_name: instance-node-exporter-{index}"),
            "    static_configs:",
            "      - targets:",
            &format!("        - {ip}:{}", NodeExporter::DEFAULT_PORT),
        ]
        .join("\n")
    }
}

pub struct Grafana;

impl Grafana {
    /// The path to the datasources directory.
    const DATASOURCES_PATH: &'static str = "/etc/grafana/provisioning/datasources";
    /// The default grafana port.
    pub const DEFAULT_PORT: u16 = 3000;

    /// The commands to install grafana.
    pub fn install_commands() -> Vec<String> {
        vec![
            "sudo apt-get install -y apt-transport-https software-properties-common wget".into(),
            "sudo wget -q -O /etc/apt/keyrings/grafana.key https://apt.grafana.com/gpg.key".into(),
            "(sudo rm /etc/apt/sources.list.d/grafana.list || true)".into(),
            "echo \
            \"deb [signed-by=/etc/apt/keyrings/grafana.key] https://apt.grafana.com stable main\" \
            | sudo tee -a /etc/apt/sources.list.d/grafana.list"
                .into(),
            "sudo apt-get update".into(),
            "sudo apt-get install -y grafana".into(),
            "sudo chmod 777 -R /etc/grafana/".into(),
        ]
    }

    /// Generate the commands to update the grafana datasource and restart grafana.
    pub fn setup_commands() -> Vec<String> {
        vec![
            format!("(rm -r {} || true)", Self::DATASOURCES_PATH),
            format!("mkdir -p {}", Self::DATASOURCES_PATH),
            format!(
                "sudo echo \"{}\" > {}/testbed.yml",
                Self::datasource(),
                Self::DATASOURCES_PATH
            ),
            "sudo service grafana-server restart".into(),
        ]
    }

    /// Generate the content of the datasource file for the given instance.
    /// NOTE: The datasource file is a yaml file so spaces are important.
    fn datasource() -> String {
        [
            "apiVersion: 1",
            "deleteDatasources:",
            "  - name: testbed",
            "    orgId: 1",
            "datasources:",
            "  - name: testbed",
            "    type: prometheus",
            "    access: proxy",
            "    orgId: 1",
            &format!("    url: http://localhost:{}", Prometheus::DEFAULT_PORT),
            "    editable: true",
            "    uid: Fixed-UID-testbed",
        ]
        .join("\n")
    }
}

#[allow(dead_code)] // TODO(Alberto): Will be used to observe local testbeds (#236)
/// Bootstrap the grafana with datasource to connect to the given instances.
/// NOTE: Only for macOS. Grafana must be installed through homebrew (and not from source).
/// Deeper grafana configuration can be done through the grafana.ini file
/// (/opt/homebrew/etc/grafana/grafana.ini) or the plist file
/// (~/Library/LaunchAgents/homebrew.mxcl.grafana.plist).
pub struct LocalGrafana;

#[allow(dead_code)] // TODO(Alberto): Will be used to observe local testbeds (#236)
impl LocalGrafana {
    /// The default grafana home directory (macOS, homebrew install).
    const DEFAULT_GRAFANA_HOME: &'static str = "/opt/homebrew/opt/grafana/share/grafana/";
    /// The path to the datasources directory.
    const DATASOURCES_PATH: &'static str = "conf/provisioning/datasources/";
    /// The default grafana port.
    pub const DEFAULT_PORT: u16 = 3000;

    /// Configure grafana to connect to the given instances. Only for macOS.
    pub fn run<I>(instances: I) -> MonitorResult<()>
    where
        I: IntoIterator<Item = Instance>,
    {
        let path: PathBuf = [Self::DEFAULT_GRAFANA_HOME, Self::DATASOURCES_PATH]
            .iter()
            .collect();

        // Remove the old datasources.
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir(&path).unwrap();

        // Create the new datasources.
        for (i, instance) in instances.into_iter().enumerate() {
            let mut file = path.clone();
            file.push(format!("instance-{}.yml", i));
            fs::write(&file, Self::datasource(&instance, i)).map_err(|e| {
                MonitorError::GrafanaError(format!("Failed to write grafana datasource ({e})"))
            })?;
        }

        // Restart grafana.
        std::process::Command::new("brew")
            .arg("services")
            .arg("restart")
            .arg("grafana")
            .arg("-q")
            .spawn()
            .map_err(|e| MonitorError::GrafanaError(e.to_string()))?;

        Ok(())
    }

    /// Generate the content of the datasource file for the given instance. This grafana instance
    /// takes one datasource per instance and assumes one prometheus server runs per instance.
    /// NOTE: The datasource file is a yaml file so spaces are important.
    fn datasource(instance: &Instance, index: usize) -> String {
        [
            "apiVersion: 1",
            "deleteDatasources:",
            &format!("  - name: instance-{index}"),
            "    orgId: 1",
            "datasources:",
            &format!("  - name: instance-{index}"),
            "    type: prometheus",
            "    access: proxy",
            "    orgId: 1",
            &format!(
                "    url: http://{}:{}",
                instance.main_ip,
                Prometheus::DEFAULT_PORT
            ),
            "    editable: true",
            &format!("    uid: UID-{index}"),
        ]
        .join("\n")
    }
}

/// Generate the commands to setup node exporter on the given instances.
struct NodeExporter;

impl NodeExporter {
    const RELEASE: &'static str = "0.18.1";
    const DEFAULT_PORT: u16 = 9200;
    const SERVICE_PATH: &'static str = "/etc/systemd/system/node_exporter.service";

    pub fn install_commands() -> Vec<String> {
        let build = format!("node_exporter-{}.linux-amd64", Self::RELEASE);
        let source = format!(
            "https://github.com/prometheus/node_exporter/releases/download/v{}/{build}.tar.gz",
            Self::RELEASE
        );

        vec![
            "(sudo systemctl status node_exporter && exit 0)".into(),
            format!("curl -LO {source}"),
            format!(
                "tar -xvf node_exporter-{}.linux-amd64.tar.gz",
                Self::RELEASE
            ),
            format!(
                "sudo mv node_exporter-{}.linux-amd64/node_exporter /usr/local/bin/",
                Self::RELEASE
            ),
            "sudo useradd -rs /bin/false node_exporter || true".into(),
            format!(
                "sudo echo \"{}\" > {}",
                Self::service_config(),
                Self::SERVICE_PATH
            ),
            "sudo systemctl daemon-reload".into(),
            "sudo systemctl start node_exporter".into(),
            "sudo systemctl enable node_exporter".into(),
        ]
    }

    fn service_config() -> String {
        [
            "[Unit]",
            "Description=Node Exporter",
            "After=network.target",
            "[Service]",
            "User=node_exporter",
            "Group=node_exporter",
            "Type=simple",
            &format!(
                "ExecStart=/usr/local/bin/node_exporter --web.listen-address=:{}",
                Self::DEFAULT_PORT
            ),
            "[Install]",
            "WantedBy=multi-user.target",
        ]
        .join("\n")
    }
}

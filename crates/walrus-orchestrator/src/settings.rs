// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, DurationSeconds, serde_as};

use crate::{
    client::Instance,
    error::{SettingsError, SettingsResult},
};

/// The git repository holding the codebase.
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Repository {
    /// The url of the repository.
    #[serde_as(as = "DisplayFromStr")]
    pub url: Url,
    /// The commit (or branch name) to deploy.
    pub commit: String,
}

impl Default for Repository {
    fn default() -> Self {
        Self {
            url: Url::parse("https://example.com/author/repo").unwrap(),
            commit: "main".into(),
        }
    }
}

impl Repository {
    /// Set the commit to 'unknown'. This options is useful when the orchestrator cannot
    /// be certain of the commit that is running on the instances. This is a failsafe against
    /// reporting wrong commit values in the measurements.
    pub fn set_unknown_commit(&mut self) {
        self.commit = "unknown".into();
    }

    /// Remove the Github access token from the repository url.
    pub fn remove_access_token(&mut self) {
        self.url.set_password(None).unwrap();
        self.url.set_username("").unwrap();
    }
}

/// The list of supported cloud providers.
#[derive(Serialize, Deserialize, Clone, Default)]
pub enum CloudProvider {
    #[default]
    #[serde(alias = "aws")]
    Aws,
    #[serde(alias = "vultr")]
    Vultr,
}

/// The testbed settings. Those are typically specified in a file.
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Settings {
    /// The testbed unique id. This allows multiple users to run concurrent testbeds on the
    /// same cloud provider's account without interference with each others.
    pub testbed_id: String,
    /// The cloud provider hosting the testbed.
    pub cloud_provider: CloudProvider,
    /// The path to the secret token for authentication with the cloud provider.
    #[serde(skip_serializing)]
    pub token_file: PathBuf,
    /// The ssh private key to access the instances.
    #[serde(
        skip_serializing,
        deserialize_with = "walrus_utils::config::resolve_home_dir"
    )]
    pub ssh_private_key_file: PathBuf,
    /// The corresponding ssh public key registered on the instances. If not specified. the
    /// public key defaults the same path as the private key with an added extension 'pub'.
    #[serde(deserialize_with = "walrus_utils::config::resolve_home_dir_option")]
    pub ssh_public_key_file: Option<PathBuf>,
    /// The list of cloud provider regions to deploy the testbed.
    pub regions: Vec<String>,
    /// The specs of the instances to deploy. Those are dependent on the cloud provider, e.g.,
    /// specifying 't3.medium' creates instances with 2 vCPU and 4GBo of ram on AWS.
    pub specs: String,
    /// The details of the git reposit to deploy.
    pub repository: Repository,
    /// The path to the client's configuration file. If not specified, the orchestrator uses the
    /// default configurations.
    pub client_parameters_path: Option<String>,
    /// The duration of the benchmark. The orchestrator stops the benchmark after this duration.
    /// If this value is set to zero, the orchestrator runs the benchmark indefinitely.
    #[serde(default = "defaults::default_benchmark_duration")]
    #[serde_as(as = "DurationSeconds")]
    pub benchmark_duration: Duration,
    /// The working directory on the remote instance (containing all configuration files).
    #[serde(default = "defaults::default_working_dir")]
    pub working_dir: PathBuf,
    /// The directory (on the local machine) where to save benchmarks measurements.
    #[serde(default = "defaults::default_results_dir")]
    pub results_dir: PathBuf,
    /// The directory (on the local machine) where to download logs files from the instances.
    #[serde(default = "defaults::default_logs_dir")]
    pub logs_dir: PathBuf,
    /// Whether to use NVMe drives for data storage (if available).
    #[serde(default = "defaults::default_use_nvme")]
    pub nvme: bool,
    /// The interval between measurements collection.
    #[serde(default = "defaults::default_scrape_interval")]
    #[serde_as(as = "DurationSeconds")]
    pub scrape_interval: Duration,
    /// Whether to downloading and analyze the client and node log files.
    #[serde(default = "defaults::default_log_processing")]
    pub log_processing: bool,
    /// Number of instances running only load generators (not nodes). If this value is set
    /// to zero, the orchestrator runs a load generate collocated with each node.
    #[serde(default = "defaults::default_dedicated_clients")]
    pub dedicated_clients: usize,
    /// Whether to start a grafana and prometheus instance on a dedicate machine.
    #[serde(default = "defaults::default_monitoring")]
    pub monitoring: bool,
    /// The timeout duration for ssh commands (in seconds).
    #[serde(default = "defaults::default_ssh_timeout")]
    #[serde_as(as = "DurationSeconds")]
    pub ssh_timeout: Duration,
    /// The number of times the orchestrator should retry an ssh command.
    #[serde(default = "defaults::default_ssh_retries")]
    pub ssh_retries: usize,
}

mod defaults {
    use std::{path::PathBuf, time::Duration};

    pub fn default_benchmark_duration() -> Duration {
        Duration::from_secs(0)
    }

    pub fn default_working_dir() -> PathBuf {
        "~/working_dir".into()
    }

    pub fn default_results_dir() -> PathBuf {
        ["./", "results"].iter().collect()
    }

    pub fn default_logs_dir() -> PathBuf {
        ["./", "logs"].iter().collect()
    }

    pub fn default_use_nvme() -> bool {
        true
    }

    pub fn default_scrape_interval() -> Duration {
        Duration::from_secs(15)
    }

    pub fn default_log_processing() -> bool {
        false
    }

    pub fn default_dedicated_clients() -> usize {
        0
    }

    pub fn default_monitoring() -> bool {
        true
    }

    pub fn default_ssh_timeout() -> Duration {
        Duration::from_secs(30)
    }

    pub fn default_ssh_retries() -> usize {
        3
    }
}

impl Settings {
    /// Load the settings from a json file.
    pub fn load<P>(path: P) -> SettingsResult<Self>
    where
        P: AsRef<Path> + Display + Clone,
    {
        let reader = || -> Result<Self, Box<dyn std::error::Error>> {
            let data = fs::read(path.clone())?;
            let data = Self::resolve_env(&path, std::str::from_utf8(&data)?)?;
            let settings: Settings = serde_yaml::from_slice(data.as_bytes())?;

            fs::create_dir_all(&settings.results_dir)?;
            fs::create_dir_all(&settings.logs_dir)?;

            Ok(settings)
        };

        reader().map_err(|e| SettingsError::InvalidSettings {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    // Resolves ${ENV} into it's value for each env variable.
    fn resolve_env<P>(path: P, s: &str) -> Result<String, std::io::Error>
    where
        P: AsRef<Path> + Display + Clone,
    {
        let mut s = s.to_string();
        for (name, value) in env::vars() {
            s = s.replace(&format!("${{{}}}", name), &value);
        }
        if s.contains("${") {
            let error = SettingsError::InvalidSettings {
                file: path.to_string(),
                message: format!("Unresolved env variables {s} in the settings file"),
            };
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error));
        }
        Ok(s)
    }

    /// Get the name of the repository (from its url).
    pub fn repository_name(&self) -> String {
        self.repository
            .url
            .path_segments()
            .expect("Url should already be checked when loading settings")
            .collect::<Vec<_>>()[1]
            .split('.')
            .next()
            .unwrap()
            .to_string()
    }

    /// Load the secret token to authenticate with the cloud provider.
    pub fn load_token(&self) -> SettingsResult<String> {
        match fs::read_to_string(&self.token_file) {
            Ok(token) => Ok(token.trim_end_matches('\n').to_string()),
            Err(e) => Err(SettingsError::TokenFileError {
                file: self.token_file.display().to_string(),
                message: e.to_string(),
            }),
        }
    }

    /// Load the ssh public key from file.
    pub fn load_ssh_public_key(&self) -> SettingsResult<String> {
        let ssh_public_key_file = self.ssh_public_key_file.clone().unwrap_or_else(|| {
            let mut private = self.ssh_private_key_file.clone();
            private.set_extension("pub");
            private
        });
        match fs::read_to_string(&ssh_public_key_file) {
            Ok(token) => Ok(token.trim_end_matches('\n').to_string()),
            Err(e) => Err(SettingsError::SshPublicKeyFileError {
                file: ssh_public_key_file.display().to_string(),
                message: e.to_string(),
            }),
        }
    }

    /// Check whether the input instance matches the criteria described in the settings.
    pub fn filter_instances(&self, instance: &Instance) -> bool {
        self.regions.contains(&instance.region)
            && instance.specs.to_lowercase().replace('.', "")
                == self.specs.to_lowercase().replace('.', "")
    }

    /// The number of regions specified in the settings.
    #[cfg(test)]
    pub fn number_of_regions(&self) -> usize {
        self.regions.len()
    }

    /// Test settings for unit tests.
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        // Create a temporary public key file.
        let mut path = tempfile::tempdir().unwrap().keep();
        path.push("test_public_key.pub");
        let public_key = "This is a fake public key for tests";
        fs::write(&path, public_key).unwrap();

        // Return set settings.
        Self {
            testbed_id: "testbed".into(),
            token_file: "/path/to/token/file".into(),
            ssh_private_key_file: "/path/to/private/key/file".into(),
            ssh_public_key_file: Some(path),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod test {
    use reqwest::Url;

    use crate::settings::Settings;

    #[test]
    fn load_ssh_public_key() {
        let settings = Settings::new_for_test();
        let public_key = settings.load_ssh_public_key().unwrap();
        assert_eq!(public_key, "This is a fake public key for tests");
    }

    #[test]
    fn repository_name() {
        let mut settings = Settings::new_for_test();
        settings.repository.url = Url::parse("https://example.com/author/name").unwrap();
        assert_eq!(settings.repository_name(), "name");
    }

    #[test]
    fn remove_access_token() {
        let mut settings = Settings::new_for_test();
        settings.repository.url = Url::parse("https://TOKEN@example.com/author/name").unwrap();
        settings.repository.remove_access_token();
        assert_eq!(
            settings.repository.url,
            Url::parse("https://example.com/author/name").unwrap()
        );
    }
}

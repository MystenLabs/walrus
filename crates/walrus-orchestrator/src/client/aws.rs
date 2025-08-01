// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use aws_config::{BehaviorVersion, Region};
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_sdk_ec2::{
    error::SdkError,
    meta::PKG_VERSION,
    primitives::Blob,
    types::{
        EphemeralNvmeSupport,
        Instance as AwsInstance,
        ResourceType,
        VolumeType,
        builders::{
            BlockDeviceMappingBuilder,
            EbsBlockDeviceBuilder,
            FilterBuilder,
            TagBuilder,
            TagSpecificationBuilder,
        },
    },
};
use serde::Serialize;

use super::{Instance, ServerProviderClient};
use crate::{
    error::{CloudProviderError, CloudProviderResult},
    settings::Settings,
};

// Make a request error from an AWS error message.
impl<T> From<SdkError<T>> for CloudProviderError
where
    T: Debug + std::error::Error + Send + Sync + 'static,
{
    fn from(e: SdkError<T>) -> Self {
        Self::RequestError(format!("{:?}", e.into_source()))
    }
}

/// An AWS client.
pub struct AwsClient {
    /// The settings of the testbed.
    settings: Settings,
    /// A list of clients, one per AWS region.
    clients: HashMap<String, aws_sdk_ec2::Client>,
}

impl Display for AwsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AWS EC2 client v{PKG_VERSION}")
    }
}

impl AwsClient {
    const OS_IMAGE: &'static str =
        "Canonical, Ubuntu, 24.04 LTS, amd64 noble image build on 2024-04-23";
    const DEFAULT_EBS_SIZE_GB: i32 = 500; // Default size of the EBS volume in GB.

    /// Make a new AWS client.
    pub async fn new(settings: Settings) -> Self {
        let profile_files = EnvConfigFiles::builder()
            .with_file(EnvConfigFileKind::Credentials, &settings.token_file)
            .with_contents(EnvConfigFileKind::Config, "[default]\noutput=json")
            .build();

        let mut clients = HashMap::new();
        for region in settings.regions.clone() {
            let sdk_config = aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region.clone()))
                .profile_files(profile_files.clone())
                .load()
                .await;
            let client = aws_sdk_ec2::Client::new(&sdk_config);
            clients.insert(region, client);
        }

        Self { settings, clients }
    }

    /// Parse an AWS response and ignore errors if they mean a request is a duplicate.
    fn check_but_ignore_duplicates<T, E>(
        response: Result<T, SdkError<E>>,
    ) -> CloudProviderResult<()>
    where
        E: Debug + std::error::Error + Send + Sync + 'static,
    {
        if let Err(e) = response {
            let error_message = format!("{e:?}");
            if !error_message.to_lowercase().contains("duplicate") {
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Convert an AWS instance into an orchestrator instance (used in the rest of the codebase).
    fn make_instance(&self, region: String, aws_instance: &AwsInstance) -> Instance {
        Instance {
            id: aws_instance
                .instance_id()
                .expect("AWS instance should have an id")
                .into(),
            region,
            main_ip: aws_instance
                .public_ip_address()
                .unwrap_or("0.0.0.0") // Stopped instances do not have an ip address.
                .parse()
                .expect("AWS instance should have a valid ip"),
            tags: vec![self.settings.testbed_id.clone()],
            specs: format!(
                "{:?}",
                aws_instance
                    .instance_type()
                    .expect("AWS instance should have a type")
            ),
            status: format!(
                "{:?}",
                aws_instance
                    .state()
                    .expect("AWS instance should have a state")
                    .name()
                    .expect("AWS status should have a name")
            )
            .as_str()
            .into(),
        }
    }

    /// Query the image id determining the os of the instances.
    /// NOTE: The image id changes depending on the region.
    async fn find_image_id(&self, client: &aws_sdk_ec2::Client) -> CloudProviderResult<String> {
        // Query all images that match the description.
        let request = client.describe_images().filters(
            FilterBuilder::default()
                .name("description")
                .values(Self::OS_IMAGE)
                .build(),
        );
        let response = request.send().await?;

        // Parse the response to select the first returned image id.
        response
            .images()
            .first()
            .ok_or_else(|| CloudProviderError::RequestError("Cannot find image id".into()))?
            .image_id
            .clone()
            .ok_or_else(|| {
                CloudProviderError::UnexpectedResponse(
                    "Received image description without id".into(),
                )
            })
    }

    /// Create a new security group for the instance (if it doesn't already exist).
    async fn create_security_group(&self, client: &aws_sdk_ec2::Client) -> CloudProviderResult<()> {
        // Create a security group (if it doesn't already exist).
        let request = client
            .create_security_group()
            .group_name(&self.settings.testbed_id)
            .description("Allow all traffic (used for benchmarks).");

        let response = request.send().await;
        Self::check_but_ignore_duplicates(response)?;

        // Authorize all traffic on the security group.
        for protocol in ["tcp", "udp", "icmp", "icmpv6"] {
            let mut request = client
                .authorize_security_group_ingress()
                .group_name(&self.settings.testbed_id)
                .ip_protocol(protocol)
                .cidr_ip("0.0.0.0/0");
            if protocol == "icmp" || protocol == "icmpv6" {
                request = request.from_port(-1).to_port(-1);
            } else {
                request = request.from_port(0).to_port(65535);
            }

            let response = request.send().await;
            Self::check_but_ignore_duplicates(response)?;
        }
        Ok(())
    }

    /// Return the command to mount the first (standard) NVMe drive.
    fn nvme_mount_command(&self) -> Vec<String> {
        const DRIVE: &str = "nvme1n1";
        let directory = self.settings.working_dir.display();
        vec![
            format!("(sudo mkfs.ext4 -E nodiscard /dev/{DRIVE} || true)"),
            format!("(sudo mount /dev/{DRIVE} {directory} || true)"),
            format!("sudo chmod 777 -R {directory}"),
        ]
    }

    fn nvme_unmount_command(&self) -> Vec<String> {
        let directory = self.settings.working_dir.display();
        vec![format!("(sudo umount {directory} || true)")]
    }

    /// Check whether the instance type specified in the settings supports NVMe drives.
    async fn check_nvme_support(&self) -> CloudProviderResult<bool> {
        // Get the client for the first region. A given instance type should either have NVMe
        // support in all regions or in none.
        let client = match self
            .settings
            .regions
            .first()
            .and_then(|x| self.clients.get(x))
        {
            Some(client) => client,
            None => return Ok(false),
        };

        // Request storage details for the instance type specified in the settings.
        let request = client
            .describe_instance_types()
            .instance_types(self.settings.specs.as_str().into());

        // Send the request.
        let response = request.send().await?;

        // Return true if the response contains references to NVMe drives.
        if let Some(info) = response.instance_types().first()
            && let Some(info) = info.instance_storage_info()
            && info.nvme_support() == Some(&EphemeralNvmeSupport::Required)
        {
            return Ok(true);
        }
        Ok(false)
    }
}

impl ServerProviderClient for AwsClient {
    const USERNAME: &'static str = "ubuntu";

    async fn list_instances(&self) -> CloudProviderResult<Vec<Instance>> {
        let filter = FilterBuilder::default()
            .name("tag:Name")
            .values(self.settings.testbed_id.clone())
            .build();

        let mut instances = Vec::new();
        for (region, client) in &self.clients {
            let request = client.describe_instances().filters(filter.clone());
            for reservation in request.send().await?.reservations() {
                for instance in reservation.instances() {
                    instances.push(self.make_instance(region.clone(), instance));
                }
            }
        }

        Ok(instances)
    }

    async fn start_instances<'a, I>(&self, instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        let mut instance_ids = HashMap::new();
        for instance in instances {
            instance_ids
                .entry(&instance.region)
                .or_insert_with(Vec::new)
                .push(instance.id.clone());
        }

        for (region, client) in &self.clients {
            let ids = instance_ids.remove(&region.to_string());
            if ids.is_some() {
                client
                    .start_instances()
                    .set_instance_ids(ids)
                    .send()
                    .await?;
            }
        }
        Ok(())
    }

    async fn stop_instances<'a, I>(&self, instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        let mut instance_ids = HashMap::new();
        for instance in instances {
            instance_ids
                .entry(&instance.region)
                .or_insert_with(Vec::new)
                .push(instance.id.clone());
        }

        for (region, client) in &self.clients {
            let ids = instance_ids.remove(&region.to_string());
            if ids.is_some() {
                client.stop_instances().set_instance_ids(ids).send().await?;
            }
        }
        Ok(())
    }

    async fn create_instance<S>(&self, region: S) -> CloudProviderResult<Instance>
    where
        S: Into<String> + Serialize + Send,
    {
        let region = region.into();
        let testbed_id = &self.settings.testbed_id;

        let client = self.clients.get(&region).ok_or_else(|| {
            CloudProviderError::RequestError(format!("Undefined region {region:?}"))
        })?;

        // Create a security group (if needed).
        self.create_security_group(client).await?;

        // Query the image id.
        let image_id = self.find_image_id(client).await?;

        // Create a new instance.
        let tags = TagSpecificationBuilder::default()
            .resource_type(ResourceType::Instance)
            .tags(TagBuilder::default().key("Name").value(testbed_id).build())
            .build();

        let storage = BlockDeviceMappingBuilder::default()
            .device_name("/dev/sda1")
            .ebs(
                EbsBlockDeviceBuilder::default()
                    .delete_on_termination(true)
                    .volume_size(Self::DEFAULT_EBS_SIZE_GB)
                    .volume_type(VolumeType::Gp2)
                    .build(),
            )
            .build();

        let request = client
            .run_instances()
            .image_id(image_id)
            .instance_type(self.settings.specs.as_str().into())
            .key_name(testbed_id)
            .min_count(1)
            .max_count(1)
            .security_groups(&self.settings.testbed_id)
            .block_device_mappings(storage)
            .tag_specifications(tags);

        let response = request.send().await?;
        let instance = &response
            .instances()
            .first()
            .expect("AWS instances list should contain instances");

        Ok(self.make_instance(region, instance))
    }

    async fn delete_instance(&self, instance: Instance) -> CloudProviderResult<()> {
        let client = self.clients.get(&instance.region).ok_or_else(|| {
            CloudProviderError::RequestError(format!("Undefined region {:?}", instance.region))
        })?;

        client
            .terminate_instances()
            .set_instance_ids(Some(vec![instance.id.clone()]))
            .send()
            .await?;

        Ok(())
    }

    async fn register_ssh_public_key(&self, public_key: String) -> CloudProviderResult<()> {
        for client in self.clients.values() {
            let request = client
                .import_key_pair()
                .key_name(&self.settings.testbed_id)
                .public_key_material(Blob::new::<String>(public_key.clone()));

            let response = request.send().await;
            Self::check_but_ignore_duplicates(response)?;
        }
        Ok(())
    }

    async fn instance_setup_commands(&self) -> CloudProviderResult<Vec<String>> {
        if self.settings.nvme && self.check_nvme_support().await? {
            Ok(self.nvme_mount_command())
        } else {
            Ok(self.nvme_unmount_command())
        }
    }
}

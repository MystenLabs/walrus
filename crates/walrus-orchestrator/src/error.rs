// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

pub type SettingsResult<T> = Result<T, SettingsError>;

#[derive(thiserror::Error, Debug)]
pub enum SettingsError {
    #[error("Failed to read settings file '{file:?}': {message}")]
    InvalidSettings { file: String, message: String },

    #[error("Failed to read token file '{file:?}': {message}")]
    TokenFileError { file: String, message: String },

    #[error("Failed to read ssh public key file '{file:?}': {message}")]
    SshPublicKeyFileError { file: String, message: String },
}

pub type CloudProviderResult<T> = Result<T, CloudProviderError>;

#[derive(thiserror::Error, Debug)]
pub enum CloudProviderError {
    #[error("Failed to send server request: {0}")]
    RequestError(String),

    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),

    #[error("Received error status code ({0}): {1}")]
    FailureResponseCode(String, String),

    #[error("SSH key \"{0}\" not found")]
    SshKeyNotFound(String),
}

pub type SshResult<T> = Result<T, SshError>;

#[derive(thiserror::Error, Debug)]
pub enum SshError {
    #[error("Failed to create ssh session with {address}: {error}")]
    SessionError {
        address: SocketAddr,
        error: ssh2::Error,
    },

    #[error("Failed to connect to instance {address}: {error}")]
    ConnectionError {
        address: SocketAddr,
        error: std::io::Error,
    },

    #[error("Remote execution on {address} returned exit code ({code}): {message}")]
    NonZeroExitCode {
        address: SocketAddr,
        code: i32,
        message: String,
    },
}

pub type MonitorResult<T> = Result<T, MonitorError>;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct MonitorError(#[from] SshError);

pub type TestbedResult<T> = Result<T, TestbedError>;

#[derive(thiserror::Error, Debug)]
pub enum TestbedError {
    #[error(transparent)]
    SettingsError(#[from] SettingsError),

    #[error(transparent)]
    CloudProviderError(#[from] CloudProviderError),

    #[error(transparent)]
    SshError(#[from] SshError),

    #[error("Not enough instances: missing {0} instances")]
    InsufficientCapacity(usize),

    #[error(transparent)]
    MonitorError(#[from] MonitorError),
}

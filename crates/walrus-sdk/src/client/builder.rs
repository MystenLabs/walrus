// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::{net::SocketAddr, sync::Arc, time::Duration};

use reqwest::{ClientBuilder as ReqwestClientBuilder, Url};
use rustls::pki_types::CertificateDer;
use rustls_native_certs::CertificateResult;
use walrus_core::NetworkPublicKey;

use crate::{
    client::{Client, UrlEndpoints},
    error::{BuildErrorKind, ClientBuildError},
    tls::TlsCertificateVerifier,
};

/// A builder that can be used to construct a [`Client`].
///
/// Can be created with [`Client::builder()`].
#[derive(Debug, Default)]
pub struct ClientBuilder {
    inner: ReqwestClientBuilder,
    server_public_key: Option<NetworkPublicKey>,
    roots: Vec<CertificateDer<'static>>,
    no_built_in_root_certs: bool,
    connect_timeout: Option<Duration>,
}

impl ClientBuilder {
    /// Default timeout that is configured for connecting to the remote server.
    ///
    /// Modern advice is that TCP implementations should have a retry timeout of 1 second
    /// (previously, it was 3 seconds). In the event of a lossy network, the SYN/SYN-ACK
    /// packets may be lost necessitating one or several retries until actually connecting to the
    /// server. We therefore want to allow for several retries.
    ///
    /// The default of 5 seconds should allow for around 2-3 SYN attempts before failing.
    ///
    /// See RFC6298 for more information.
    const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Creates a new builder to construct a [`Client`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new builder using an existing request client builder.
    ///
    /// This can be used to preconfigure options on the client. These options may be overwritten,
    /// however, during construction of the final client.
    ///
    /// TLS settings are not preserved.
    pub fn from_reqwest(builder: ReqwestClientBuilder) -> Self {
        Self {
            inner: builder,
            ..Self::default()
        }
    }

    /// Authenticate the server with the provided public key instead of the Web PKI.
    ///
    /// By default, to authenticate the connection to the storage node, the client verifies that the
    /// storage node provides a valid, unexpired certificate which matches the `authority` provided
    /// to [`build()`][Self::build]. Calling this method instead authenticates the storage node
    /// solely on the basis that it is able to establish the TLS connection with the private key
    /// corresponding to the provided public key.
    pub fn authenticate_with_public_key(mut self, public_key: NetworkPublicKey) -> Self {
        self.server_public_key = Some(public_key);
        self
    }

    /// Clears proxy settings in the client, and disables fetching proxy settings from the OS.
    ///
    /// On some systems, this can speed up the construction of the client.
    pub fn no_proxy(mut self) -> Self {
        self.inner = self.inner.no_proxy();
        self
    }

    /// Add a custom DER-encoded root certificate.
    ///
    /// It is the responsibility of the caller to check the certificate for validity.
    pub fn add_root_certificate(mut self, certificate: &[u8]) -> Self {
        self.roots
            .push(CertificateDer::from(certificate).into_owned());
        self
    }

    /// Add a custom DER-encoded root certificate.
    ///
    /// It is the responsibility of the caller to check the certificate for validity.
    pub fn add_root_certificates(mut self, certificates: &[CertificateDer<'static>]) -> Self {
        self.roots.extend(certificates.iter().cloned());
        self
    }

    /// Controls the use of built-in/preloaded certificates during certificate validation.
    ///
    /// Defaults to true – built-in system certs will be used.
    pub fn tls_built_in_root_certs(mut self, tls_built_in_root_certs: bool) -> Self {
        self.no_built_in_root_certs = !tls_built_in_root_certs;
        self
    }

    /// Set a timeout for only the connect phase of a Client.
    ///
    /// The default is 5 seconds.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Convenience function to build the client where the server is identified by a [`SocketAddr`].
    ///
    /// Equivalent `self.build(&remote.to_string())`
    pub fn build_for_remote_ip(self, remote: SocketAddr) -> Result<Client, ClientBuildError> {
        self.build(&remote.to_string())
    }

    /// Consume the `ClientBuilder` and return a configured [`Client`].
    ///
    /// This method fails if a valid URL cannot be created with the provided address, the
    /// Rustls TLS backend cannot be initialized, or the resolver cannot load the system
    /// configuration.
    pub fn build(mut self, address: &str) -> Result<Client, ClientBuildError> {
        #[cfg(msim)]
        {
            self = self.no_proxy();
        }

        let url = Url::parse(&format!("https://{address}"))
            .map_err(|_| BuildErrorKind::InvalidHostOrPort)?;
        // We extract the host from the URL, since the provided host string may have details like a
        // username or password.
        let host = url
            .host_str()
            .ok_or(BuildErrorKind::InvalidHostOrPort)?
            .to_string();
        let endpoints = UrlEndpoints(url);

        if !self.no_built_in_root_certs {
            let CertificateResult { certs, errors, .. } = rustls_native_certs::load_native_certs();
            if certs.is_empty() {
                return Err(BuildErrorKind::FailedToLoadCerts(errors).into());
            };
            if !errors.is_empty() {
                tracing::warn!(
                    "encountered {} errors when trying to load native certs",
                    errors.len(),
                );
                tracing::debug!(?errors, "errors encountered when loading native certs");
            }
            self.roots.extend(certs);
        }

        let verifier = if let Some(public_key) = self.server_public_key {
            TlsCertificateVerifier::new_with_pinned_public_key(public_key, host, self.roots)
                .map_err(BuildErrorKind::Tls)?
        } else {
            TlsCertificateVerifier::new(self.roots).map_err(BuildErrorKind::Tls)?
        };

        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();

        let inner = self
            .inner
            .https_only(true)
            .http2_prior_knowledge()
            .http2_adaptive_window(true)
            .use_preconfigured_tls(rustls_config)
            .connect_timeout(
                self.connect_timeout
                    .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT),
            )
            .build()
            .map_err(ClientBuildError::reqwest)?;

        Ok(Client { inner, endpoints })
    }
}

// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::IpAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use p256::elliptic_curve::ALGORITHM_OID;
use rustls::{
    DigitallySignedStruct,
    RootCertStore,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use walrus_core::{self, NetworkPublicKey};
use x509_cert::{
    Version,
    certificate::{Certificate as X509Certificate, TbsCertificateInner},
    der::{
        Decode,
        Encode,
        EncodePem as _,
        asn1::{BitString, GeneralizedTime, Ia5String},
        pem::LineEnding,
    },
    ext::{
        AsExtension,
        pkix::{SubjectAltName, name::GeneralName},
    },
    name::DistinguishedName,
    serial_number::SerialNumber,
    spki::{AlgorithmIdentifierOwned, SubjectPublicKeyInfo},
    time::{Time, Validity},
};

/// Create a unsigned, self-signed TLS certificate for a public key.
///
/// The subject (and issuer) is the storage node identified by
/// `walrus_core::server_name_from_public_key(public_key)`, whose alternative name is provided by
/// `subject_alt_name`. The alternative name should correspond to the DNS name or IP-address to
///  which the connection is established, so that the certificate can be used to verify the
///  connection.
///
/// When using the Rustls TLS backend, a certificate generated in this way can be used to establish
/// a root of trust for securing TLS connections, as Rustls assumes that the certificate's
/// signature, expiry, etc. has already been verified.
pub fn create_unsigned_certificate(
    public_key: &NetworkPublicKey,
    subject_alt_name: GeneralName,
) -> CertificateDer<'static> {
    static NEXT_SERIAL_NUMBER: AtomicU64 = AtomicU64::new(1);
    let serial_number: SerialNumber = NEXT_SERIAL_NUMBER.fetch_add(1, Ordering::SeqCst).into();

    let (issuer_and_subject_name, generated_alt_name) = generate_names_from_public_key(public_key);

    let alt_name_extension = SubjectAltName(vec![generated_alt_name, subject_alt_name])
        .to_extension(&issuer_and_subject_name, &[])
        .expect("hard-coded extension is valid");

    let public_key_bytes: p256::PublicKey = public_key.pubkey.into();

    let signature_algorithm = AlgorithmIdentifierOwned {
        oid: ALGORITHM_OID,
        parameters: None,
    };
    let tbs_certificate = TbsCertificateInner {
        version: Version::V3,
        serial_number,
        issuer: issuer_and_subject_name.clone(),
        subject: issuer_and_subject_name.clone(),
        subject_public_key_info: SubjectPublicKeyInfo::from_key(public_key_bytes)
            .expect("info can always be constructed from NetworkPublicKey"),
        validity: Validity {
            not_before: Time::GeneralTime(
                GeneralizedTime::from_unix_duration(Duration::ZERO).expect("epoch is valid time"),
            ),
            not_after: Time::INFINITY,
        },
        issuer_unique_id: None,
        subject_unique_id: None,
        signature: signature_algorithm.clone(),
        extensions: Some(vec![alt_name_extension]),
    };

    let certificate = X509Certificate {
        tbs_certificate,
        signature_algorithm,
        signature: BitString::from_bytes(&[]).expect("an empty bit string can always be created"),
    };
    tracing::trace!(
        certificate = certificate
            .to_pem(LineEnding::default())
            .expect("generate certificate has valid pem"),
        "generated certificate"
    );
    let der = certificate
        .to_der()
        .expect("self-signed certificate is DER-encodable");
    CertificateDer::from(der)
}

fn parse_subject_alt_name(server_name: String) -> Result<GeneralName, ()> {
    if let Ok(ip_address) = IpAddr::from_str(&server_name) {
        Ok(ip_address.into())
    } else if let Ok(ascii_string) = Ia5String::try_from(server_name) {
        Ok(GeneralName::DnsName(ascii_string))
    } else {
        Err(())
    }
}

fn generate_names_from_public_key(
    public_key: &NetworkPublicKey,
) -> (DistinguishedName, GeneralName) {
    let generated_server_name = crate::server_name_from_public_key(public_key);
    let distinguished_name = DistinguishedName::from_str(&format!("CN={generated_server_name}"))
        .expect("static name is always valid");

    let alt_name_string = Ia5String::try_from(generated_server_name)
        .expect("generate subject is always a valid Ia5String");
    let alt_name = GeneralName::DnsName(alt_name_string);

    (distinguished_name, alt_name)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VerifierBuildError {
    #[error("the provided subject name is neither an IP or DNS name")]
    InvalidSubject,
    #[error("unable to load trusted certificates from the OS: {0}")]
    FailedToLoadCerts(#[from] std::io::Error),
    #[error(transparent)]
    Builder(#[from] rustls::client::VerifierBuilderError),
}

#[derive(Debug)]
pub(crate) struct TlsCertificateVerifier {
    inner: WebPkiServerVerifier,
    public_key: Option<NetworkPublicKey>,
}

impl TlsCertificateVerifier {
    /// Returns a new verifier that can be used to verify server certificates.
    pub(crate) fn new(roots: Vec<CertificateDer<'static>>) -> Result<Self, VerifierBuildError> {
        Self::new_with_optional_key(None, roots)
    }

    /// Returns a new verifier that only established connections with servers that present the
    /// expected public key.
    pub(crate) fn new_with_pinned_public_key(
        public_key: NetworkPublicKey,
        subject_name: String,
        roots: Vec<CertificateDer<'static>>,
    ) -> Result<Self, VerifierBuildError> {
        Self::new_with_optional_key(Some((public_key, subject_name)), roots)
    }

    fn new_with_optional_key(
        name_and_key: Option<(NetworkPublicKey, String)>,
        roots: Vec<CertificateDer<'static>>,
    ) -> Result<Self, VerifierBuildError> {
        let mut trust_root = RootCertStore::empty();

        let public_key = if let Some((public_key, subject_name)) = name_and_key {
            tracing::debug!(
                walrus.node.public_key = %public_key,
                subject_name = subject_name,
                "requiring public key for connections"
            );
            let subject_name =
                parse_subject_alt_name(subject_name).or(Err(VerifierBuildError::InvalidSubject))?;
            let certificate = create_unsigned_certificate(&public_key, subject_name);

            trust_root
                .add(certificate)
                .expect("generated certificate is valid");

            Some(public_key)
        } else {
            None
        };

        trust_root.add_parsable_certificates(roots);
        let inner: Arc<_> = WebPkiServerVerifier::builder(trust_root.into()).build()?;
        let inner = Arc::into_inner(inner).expect("uniquely owned arc");

        Ok(Self { inner, public_key })
    }

    /// Returns true if the a public key has been pinned AND the same key is in the provided
    /// end entity certificate, false otherwise.
    fn has_pinned_public_key(&self, end_entity: &CertificateDer<'_>) -> bool {
        let Some(ref expected_key) = self.public_key else {
            return false;
        };

        let Ok(certificate) = X509Certificate::from_der(end_entity) else {
            tracing::warn!("unable to parse certificate issued by server");
            return false;
        };
        let certificate = certificate.tbs_certificate;

        let public_key_info = certificate.subject_public_key_info;
        let expected_subject_info =
            SubjectPublicKeyInfo::from_key(p256::PublicKey::from(expected_key.pubkey))
                .expect("info can always be constructed from NetworkPublicKey");

        expected_subject_info == public_key_info
    }

    /// Returns true if the a public key has NOT been pinned OR it has been pinned and the same key
    /// is in the provided end entity certificate, false otherwise.
    fn has_pinned_public_key_or_unset(&self, end_entity: &CertificateDer<'_>) -> bool {
        self.public_key.is_none() || self.has_pinned_public_key(end_entity)
    }
}

impl ServerCertVerifier for TlsCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let result = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        );

        match result {
            Ok(verified) if !self.has_pinned_public_key_or_unset(end_entity) => {
                Err(rustls::Error::General(
                    "the server's certificate does not match the pinned public key".to_owned(),
                ))
            }
            // If the verification failed due to the certificate having expired, check if the
            // key is the pinned key, and if so, accept it. Note that this just accepts the
            // certificate, the key is verified otherwise as part of the handshake.
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::Expired))
            | Err(rustls::Error::InvalidCertificate(rustls::CertificateError::ExpiredContext {
                ..
            })) if self.has_pinned_public_key(end_entity) => Ok(ServerCertVerified::assertion()),
            _ => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

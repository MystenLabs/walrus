// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::IpAddr,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use p256::elliptic_curve::ALGORITHM_OID;
use reqwest::Certificate;
use walrus_core::{self, NetworkPublicKey};
use x509_cert::{
    certificate::{Certificate as X509Certificate, TbsCertificateInner},
    der::{
        asn1::{BitString, GeneralizedTime, Ia5String},
        Encode,
    },
    ext::{
        pkix::{name::GeneralName, SubjectAltName},
        AsExtension,
    },
    name::DistinguishedName,
    serial_number::SerialNumber,
    spki::{AlgorithmIdentifierOwned, SubjectPublicKeyInfo},
    time::{Time, Validity},
    Version,
};

/// Create a unsigned, self-signed TLS certificate for a public key.
///
/// The subject (and issuer) is the storage node identified by
/// `walrus_core::server_name_from_public_key(public_key)`, whose alternative name is provided by
/// `subject_alt_name`. The alternative name should correspond to the DNS name or IP-address under
/// to which the connection, so that the certificate can be used to verify the connection.
///
/// When using the Rustls TLS backend, a certificate generated in this way can be used to establish
/// a root of trust for securing TLS connections, as Rustls assumes that the certificate's
/// signature, expiry, etc. has already been verified.
pub(crate) fn create_unsigned_certificate(
    public_key: &NetworkPublicKey,
    subject_alt_name: GeneralName,
) -> Certificate {
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
        signature: BitString::from_bytes(&[]).unwrap(),
    };
    let der = certificate
        .to_der()
        .expect("self signed certificate is der-encodable");
    Certificate::from_der(&der).expect("previously serialized certificate is decodable")
}

pub(crate) fn parse_subject_alt_name(server_name: String) -> Result<GeneralName, ()> {
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
    let generated_server_name = walrus_core::server_name_from_public_key(public_key);
    let distinguished_name = DistinguishedName::from_str(&format!("CN={generated_server_name}"))
        .expect("static name is always valid");

    let alt_name_string = Ia5String::try_from(generated_server_name)
        .expect("generate subject is always a valid Ia5String");
    let alt_name = GeneralName::DnsName(alt_name_string);

    (distinguished_name, alt_name)
}

//! TLS connector helper.

use native_tls::{Certificate, TlsConnector};
use std::fs;
use std::path::Path;

use crate::error::Result;

/// Build a [`TlsConnector`] based on the provided options.
///
/// - If `ca_cert` is `Some`, the CA certificate is loaded and added to the
///   connector's root certificate store.
/// - If `verify` is `false`, certificate verification is disabled entirely
///   (suitable for development only).
pub(crate) fn build_connector(
    verify: bool,
    ca_cert: Option<&Path>,
) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    if !verify {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    } else if let Some(path) = ca_cert {
        let pem = fs::read(path)?;
        let cert = Certificate::from_pem(&pem)?;
        builder.add_root_certificate(cert);
    }

    Ok(builder.build()?)
}

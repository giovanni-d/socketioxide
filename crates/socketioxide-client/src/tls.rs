use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};

use crate::errors::ConnectError;

pub(crate) fn default_config() -> Result<Arc<ClientConfig>, ConnectError> {
    #[allow(unused_mut)]
    let mut roots = RootCertStore::empty();
    #[cfg(feature = "rustls-webpki-roots")]
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(feature = "rustls-native-roots")]
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    if roots.is_empty() {
        return Err(ConnectError::Tls(
            "no trusted root certificates: enable `rustls-webpki-roots` or `rustls-native-roots`, \
             or set `ClientBuilder::tls_config`"
                .to_owned(),
        ));
    }
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| ConnectError::Tls(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(any(feature = "rustls-webpki-roots", feature = "rustls-native-roots"))]
    fn default_config_trusts_the_enabled_roots() {
        assert!(default_config().is_ok());
    }

    #[test]
    #[cfg(not(any(feature = "rustls-webpki-roots", feature = "rustls-native-roots")))]
    fn default_config_needs_roots() {
        assert!(matches!(default_config(), Err(ConnectError::Tls(_))));
    }
}

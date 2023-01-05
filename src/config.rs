//! Configuration for sqldb-postgres capability provider
//!
use std::{str::FromStr, time::Duration};
use couchbase::{Cluster, Collection, Bucket};

use serde::Deserialize;
use wasmbus_rpc::{core::LinkDefinition, error::RpcError};


const COUCHBASE_URL_KEY: &str = "URL";
const COUCHBASE_BUCKET_KEY: &str = "bucket";
const COUCHBASE_COLLECTION_KEY: &str = "collection";
const COUCHBASE_USERNAME_KEY: &str = "username";
const COUCHBASE_PASSWORD_KEY: &str = "password";

const DEFAULT_CONNECT_URL: &str = "couchbase://0.0.0.0";
const DEFAULT_BUCKET: &str = "default";
const DEFAULT_COLLECTION: &str = "_default";
const DEFAULT_USERNAME: &str = "Administrator";
const DEFAULT_PASSWORD: &str = "password";

#[derive(Debug, Default, Deserialize)]
pub(crate) struct Config {
    url: String,
    bucket: String,
    collection : String,
    username: String,
    password: String
}

impl Config {
    fn new() -> Self {
        Config {
            url: DEFAULT_CONNECT_URL.to_string(),
            bucket: DEFAULT_BUCKET.to_string(),
            collection: DEFAULT_COLLECTION.to_string(),
            username: DEFAULT_USERNAME.to_string(),
            password: DEFAULT_PASSWORD.to_string()
        }
    }
}

/// Load configuration from 'values' field of LinkDefinition.
/// Support a variety of configuration possibilities:
///  'uri' (only) - sets the uri, and uses a default connection pool
///  'config_json' - json with 'uri' and 'pool' settings
///  'config_b64' - base64-encoded json wih 'uri' and 'pool' settings
pub(crate) fn load_config(ld: &LinkDefinition) -> Result<Config, RpcError> {
    let mut config = Config::new();

        if let Some(cj) = ld.values.get("config_b64") {
        config = serde_json::from_slice(
            &base64::decode(cj)
                .map_err(|_| RpcError::ProviderInit("invalid config_base64 encoding".into()))?,
        )
            .map_err(|e| RpcError::ProviderInit(format!("invalid json config: {}", e)))?;
    }
    if let Some(cj) = ld.values.get("config_json") {
        config = serde_json::from_str(cj.as_str())
            .map_err(|e| RpcError::ProviderInit(format!("invalid json config: {}", e)))?;
    }
    if let Some(url) = ld.values.get(COUCHBASE_URL_KEY) {
        config.url = url.to_string();
    }
    if let Some(collection) = ld.values.get(COUCHBASE_COLLECTION_KEY) {
        config.collection = collection.to_string();
    }
    if let Some(bucket) = ld.values.get(COUCHBASE_BUCKET_KEY) {
        config.bucket = bucket.to_string();
    }
    if let Some(username) = ld.values.get(COUCHBASE_USERNAME_KEY) {
        config.username = username.to_string();
    }
    if let Some(password) = ld.values.get(COUCHBASE_PASSWORD_KEY) {
        config.password = password.to_string();
    }
    Ok(config)
}

// Create Couchbase collection connection
pub(crate) async fn create_collection_conection(config: Config) -> Result<crate::Collection, RpcError> {
    let cluster = Cluster::connect(config.url, config.username, config.password);

    let bucket = cluster.bucket(config.bucket);
    let collection = bucket.default_collection();
    Ok(collection)
}

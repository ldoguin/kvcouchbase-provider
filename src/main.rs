//! Couchbase implementation for wasmcloud:keyvalue.
//!
mod config;

use std::{collections::HashMap, convert::Infallible, ops::DerefMut, sync::Arc};
use std::borrow::Borrow;
use std::env::args;
use couchbase::{Collection, CouchbaseError, ExistsOptions, GetOptions, GetResult, RemoveOptions};
use couchbase::CouchbaseError::DocumentNotFound;
use futures::executor::block_on;

use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{info, instrument, warn};
use wasmbus_rpc::provider::prelude::*;
use wasmcloud_interface_keyvalue::{
    GetResponse, IncrementRequest, KeyValue, KeyValueReceiver, ListAddRequest, ListDelRequest,
    ListRangeRequest, SetAddRequest, SetDelRequest, SetRequest, StringList,
};
use crate::config::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hd = load_host_data()?;

    provider_start(
        KvCouchbaseProvider::default(),
        hd,
        Some("KeyValue Couchbase Provider".to_string()),
    )?;

    eprintln!("KVCouchbase provider exiting");
    Ok(())
}

/// Couchbase keyValue provider implementation.
#[derive(Default, Clone, Provider)]
#[services(KeyValue)]
struct KvCouchbaseProvider {
    // store couchbase connections per actor
    actors: Arc<RwLock<HashMap<String, Collection>>>,
}


/// use default implementations of provider message handlers
impl ProviderDispatch for KvCouchbaseProvider {}

/// Handle provider control commands
/// put_link (new actor link command), del_link (remove link command), and shutdown
#[async_trait]
impl ProviderHandler for KvCouchbaseProvider {
    /// Provider should perform any operations needed for a new link,
    /// including setting up per-actor resources, and checking authorization.
    /// If the link is allowed, return true, otherwise return false to deny the link.
    #[instrument(level = "debug", skip(self, ld), fields(actor_id = %ld.actor_id))]
    async fn put_link(&self, ld: &LinkDefinition) -> RpcResult<bool> {
        let config = config::load_config(ld)?;
        let collection = config::create_collection_conection(config).await.unwrap();

        let mut update_map = self.actors.write().await;
        update_map.insert(ld.actor_id.to_string(), collection);
        Ok(true)
    }

    /// Handle notification that a link is dropped - close the connection
    #[instrument(level = "info", skip(self))]
    async fn delete_link(&self, actor_id: &str) {
        let mut aw = self.actors.write().await;
        if let Some(conn) = aw.remove(actor_id) {
            info!("couchbase closing connection for actor {}", actor_id);
            drop(conn)
        }
    }

    /// Handle shutdown request by closing all connections
    async fn shutdown(&self) -> Result<(), Infallible> {
        let mut aw = self.actors.write().await;
        // empty the actor link data and stop all servers
        for (_, conn) in aw.drain() {
            drop(conn)
        }
        Ok(())
    }
}

fn to_rpc_err(e: CouchbaseError) -> RpcError {
    RpcError::Other(format!("Couchbase error: {}", e))
}

fn actor_id(ctx: &Context) -> Result<&String, RpcError> {
    ctx.actor
        .as_ref()
        .ok_or_else(|| RpcError::InvalidParameter("no actor in request".into()))
}

/// Handle KeyValue methods that interact with Couchbase
#[async_trait]
impl KeyValue for KvCouchbaseProvider {

    /// Increments a numeric value, returning the new value
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.key))]
    async fn increment(&self, ctx: &Context, arg: &IncrementRequest) -> RpcResult<i32> {
        Err(RpcError::NotImplemented)
    }

    /// Returns true if the store contains the key
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.to_string()))]
    async fn contains<TS: ToString + ?Sized + Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<bool> {
        let actor_id = actor_id(ctx)?;
        let rd = self.actors.read().await;
        let collection = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        match block_on(collection.exists(arg.to_string(), ExistsOptions::default())) {
            Ok(r) => Ok( r.exists()),
            Err(e) => Err(to_rpc_err(e)),
        }
    }

    /// Deletes a key, returning true if the key was deleted
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.to_string()))]
    async fn del<TS: ToString + ?Sized + Sync>(&self, ctx: &Context, arg: &TS) -> RpcResult<bool> {

        let actor_id = actor_id(ctx)?;
        let rd = self.actors.read().await;
        let collection = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        match block_on(collection.remove(arg.to_string(), RemoveOptions::default())) {
            Ok(r) => Ok( 1 > 0),
            Err(e) => Err(to_rpc_err(e)),
        }
    }

    /// Gets a value for a specified key. If the key exists,
    /// the return structure contains exists: true and the value,
    /// otherwise the return structure contains exists == false.
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.to_string()))]
    async fn get<TS: ToString + ?Sized + Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<GetResponse> {

        let actor_id = actor_id(ctx)?;
        let rd = self.actors.read().await;
        let collection = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        let res = block_on(collection.get(arg.to_string(), GetOptions::default()));
        if res.is_ok() {
            Ok(GetResponse {
                exists: true,
                value: res.unwrap().content().unwrap(),
            })
        } else {
            let e = res.err().unwrap();
            match e {
                DocumentNotFound  => Ok(GetResponse {
                    exists: false,
                    ..Default::default()
                })
                ,
                _ => Err(to_rpc_err(e))
            }
        }
    }

    /// Append a value onto the end of a list. Returns the new list size
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.list_name))]
    async fn list_add(&self, ctx: &Context, arg: &ListAddRequest) -> RpcResult<u32> {
        Err(RpcError::NotImplemented)
    }

    /// Deletes a list and its contents
    /// input: list name
    /// returns: true if the list existed and was deleted
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.to_string()))]
    async fn list_clear<TS: ToString + ?Sized + Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<bool> {
        // self.del(ctx, arg).await
        Err(RpcError::NotImplemented)
    }

    /// Deletes an item from a list. Returns true if the item was removed.
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.list_name))]
    async fn list_del(&self, ctx: &Context, arg: &ListDelRequest) -> RpcResult<bool> {
        Err(RpcError::NotImplemented)
    }

    /// Retrieves a range of values from a list using 0-based indices.
    /// Start and end values are inclusive, for example, (0,10) returns
    /// 11 items if the list contains at least 11 items. If the stop value
    /// is beyond the end of the list, it is treated as the end of the list.
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.list_name))]
    async fn list_range(&self, ctx: &Context, arg: &ListRangeRequest) -> RpcResult<StringList> {
        Err(RpcError::NotImplemented)
    }

    /// Sets the value of a key.
    /// expires is an optional number of seconds before the value should be automatically deleted,
    /// or 0 for no expiration.
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.key))]
    async fn set(&self, ctx: &Context, arg: &SetRequest) -> RpcResult<()> {
        Err(RpcError::NotImplemented)
    }

    /// Add an item into a set. Returns number of items added
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.set_name))]
    async fn set_add(&self, ctx: &Context, arg: &SetAddRequest) -> RpcResult<u32> {
        Err(RpcError::NotImplemented)
    }

    /// Remove a item from the set. Returns
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.set_name))]
    async fn set_del(&self, ctx: &Context, arg: &SetDelRequest) -> RpcResult<u32> {
        Err(RpcError::NotImplemented)
    }

    /// Deletes a set and its contents
    /// input: set name
    /// returns: true if the set existed and was deleted
    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.to_string()))]
    async fn set_clear<TS: ToString + ?Sized + Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<bool> {
        // self.del(ctx, arg).await
        Err(RpcError::NotImplemented)
    }

    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, keys = ?arg))]
    async fn set_intersection(
        &self,
        ctx: &Context,
        arg: &StringList,
    ) -> Result<StringList, RpcError> {
        Err(RpcError::NotImplemented)
    }

    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, key = %arg.to_string()))]
    async fn set_query<TS: ToString + ?Sized + Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<StringList> {
        Err(RpcError::NotImplemented)
    }

    #[instrument(level = "debug", skip(self, ctx, arg), fields(actor_id = ?ctx.actor, keys = ?arg))]
    async fn set_union(&self, ctx: &Context, arg: &StringList) -> RpcResult<StringList> {
        Err(RpcError::NotImplemented)
    }

}


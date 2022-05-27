// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use futures::future::join_all;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use sui_config::NetworkConfig;
use sui_config::NodeConfig;
use sui_core::authority::AuthorityState;
use sui_core::authority_active::ActiveAuthority;
use sui_core::authority_client::NetworkAuthorityClient;
use sui_core::authority_server::AuthorityServer;
use sui_core::consensus_adapter::ConsensusListener;
use sui_node::SuiNode;
use tokio::sync::mpsc::channel;
use tracing::{error, info};

pub struct SuiNetwork {
    pub spawned_authorities: Vec<SuiNode>,
}

impl SuiNetwork {
    pub async fn start(config: &NetworkConfig) -> Result<Self, anyhow::Error> {
        if config.validator_configs().is_empty() {
            return Err(anyhow!(
                "No authority configured for the network, please run genesis."
            ));
        }

        info!(
            "Starting network with {} authorities",
            config.validator_configs().len()
        );

        let mut spawned_authorities = Vec::new();
        for validator in config.validator_configs() {
            let server = SuiNode::start(validator).await?;
            spawned_authorities.push(server);
        }
        info!("Started {} authorities", spawned_authorities.len());

        Ok(Self {
            spawned_authorities,
        })
    }

    pub async fn wait_for_completion(self) -> Result<(), anyhow::Error> {
        let mut handles = Vec::new();
        for spawned_server in self.spawned_authorities {
            handles.push(async move {
                if let Err(err) = spawned_server.wait().await {
                    error!("Server ended with an error: {err}");
                }
            });
        }
        join_all(handles).await;
        info!("All servers stopped.");
        Ok(())
    }
}

/// Spawn all the subsystems run by a Sui authority: a consensus node, a sui authority server,
/// and a consensus listener bridging the consensus node and the sui authority.
pub async fn make_authority(
    validator_config: &NodeConfig,
    state: AuthorityState,
) -> Result<AuthorityServer> {
    let (tx_consensus_to_sui, rx_consensus_to_sui) = channel(1_000);
    let (tx_sui_to_consensus, rx_sui_to_consensus) = channel(1_000);

    let authority_state = Arc::new(state);

    // Spawn the consensus node of this authority.
    let consensus_config = validator_config
        .consensus_config()
        .ok_or_else(|| anyhow!("Validator is missing consensus config"))?;
    let consensus_keypair = validator_config.key_pair().make_narwhal_keypair();
    let consensus_name = consensus_keypair.name.clone();
    let consensus_store = narwhal_node::NodeStorage::reopen(consensus_config.db_path());
    narwhal_node::Node::spawn_primary(
        consensus_keypair,
        validator_config
            .committee_config()
            .narwhal_committee()
            .to_owned(),
        &consensus_store,
        consensus_config.narwhal_config().to_owned(),
        /* consensus */ true, // Indicate that we want to run consensus.
        /* execution_state */ authority_state.clone(),
        /* tx_confirmation */ tx_consensus_to_sui,
    )
    .await?;
    narwhal_node::Node::spawn_workers(
        consensus_name,
        /* ids */ vec![0], // We run a single worker with id '0'.
        validator_config
            .committee_config()
            .narwhal_committee()
            .to_owned(),
        &consensus_store,
        consensus_config.narwhal_config().to_owned(),
    );

    // Spawn a consensus listener. It listen for consensus outputs and notifies the
    // authority server when a sequenced transaction is ready for execution.
    ConsensusListener::spawn(
        rx_sui_to_consensus,
        rx_consensus_to_sui,
        /* max_pending_transactions */ 1_000_000,
    );

    // If we have network information make authority clients
    // to all authorities in the system.
    let _active_authority: Option<()> = {
        let mut authority_clients = BTreeMap::new();
        let mut config = mysten_network::config::Config::new();
        config.connect_timeout = Some(Duration::from_secs(5));
        config.request_timeout = Some(Duration::from_secs(5));
        for validator in validator_config.committee_config().validator_set() {
            let channel = config.connect_lazy(validator.network_address()).unwrap();
            let client = NetworkAuthorityClient::new(channel);
            authority_clients.insert(validator.public_key(), client);
        }

        let _active_authority = ActiveAuthority::new(authority_state.clone(), authority_clients)?;

        // TODO: turn on to start the active part of validators
        //
        // let join_handle = active_authority.spawn_all_active_processes().await;
        // Some(join_handle)
        None
    };

    // Return new authority server. It listen to users transactions and send back replies.
    Ok(AuthorityServer::new(
        validator_config.network_address().to_owned(),
        authority_state,
        consensus_config.address().to_owned(),
        /* tx_consensus_listener */ tx_sui_to_consensus,
    ))
}

#[cfg(test)]
#[path = "consensus_manager_test.rs"]
mod consensus_manager_test;

use std::collections::HashMap;
use std::sync::Arc;

use apollo_batcher_types::batcher_types::RevertBlockInput;
use apollo_batcher_types::communication::SharedBatcherClient;
use apollo_class_manager_types::transaction_converter::TransactionConverter;
use apollo_class_manager_types::SharedClassManagerClient;
use apollo_consensus::stream_handler::StreamHandler;
use apollo_consensus::types::ConsensusError;
use apollo_consensus::votes_threshold::QuorumType;
use apollo_consensus_orchestrator::cende::CendeAmbassador;
use apollo_consensus_orchestrator::sequencer_consensus_context::{
    SequencerConsensusContext,
    SequencerConsensusContextDeps,
};
use apollo_infra::component_definitions::ComponentStarter;
use apollo_infra_utils::type_name::short_type_name;
use apollo_l1_gas_price_types::L1GasPriceProviderClient;
use apollo_network::gossipsub_impl::Topic;
use apollo_network::network_manager::metrics::{BroadcastNetworkMetrics, NetworkMetrics};
use apollo_network::network_manager::{BroadcastTopicChannels, NetworkManager};
use apollo_protobuf::consensus::{HeightAndRound, ProposalPart, StreamMessage, Vote};
use apollo_reverts::revert_blocks_and_eternal_pending;
use apollo_state_sync_types::communication::SharedStateSyncClient;
use apollo_time::time::DefaultClock;
use async_trait::async_trait;
use futures::channel::mpsc;
use starknet_api::block::BlockNumber;
use tracing::{info, info_span, Instrument};

use crate::config::ConsensusManagerConfig;
use crate::metrics::{
    CONSENSUS_NUM_BLACKLISTED_PEERS,
    CONSENSUS_NUM_CONNECTED_PEERS,
    CONSENSUS_PROPOSALS_NUM_RECEIVED_MESSAGES,
    CONSENSUS_PROPOSALS_NUM_SENT_MESSAGES,
    CONSENSUS_VOTES_NUM_RECEIVED_MESSAGES,
    CONSENSUS_VOTES_NUM_SENT_MESSAGES,
};

#[derive(Clone)]
pub struct ConsensusManager {
    pub config: ConsensusManagerConfig,
    pub batcher_client: SharedBatcherClient,
    pub state_sync_client: SharedStateSyncClient,
    pub class_manager_client: SharedClassManagerClient,
    l1_gas_price_provider: Arc<dyn L1GasPriceProviderClient>,
}

use std::sync::atomic::{AtomicU64, Ordering};

use apollo_batcher_types::batcher_types::{
    CentralObjects,
    DecisionReachedInput,
    DecisionReachedResponse,
    GetHeightResponse,
    GetProposalContent,
    GetProposalContentInput,
    GetProposalContentResponse,
    ProposalStatus,
    ProposeBlockInput,
    SendProposalContent,
    SendProposalContentInput,
    SendProposalContentResponse,
    StartHeightInput,
    ValidateBlockInput,
};
use apollo_batcher_types::communication::{BatcherClient, BatcherClientResult};
use apollo_state_sync_types::state_sync_types::SyncBlock;

pub struct MockBatcherClient {
    pub height: AtomicU64,
}

#[async_trait]
impl BatcherClient for MockBatcherClient {
    async fn propose_block(&self, input: ProposeBlockInput) -> BatcherClientResult<()> {
        Ok(())
    }

    async fn get_height(&self) -> BatcherClientResult<GetHeightResponse> {
        Ok(GetHeightResponse { height: BlockNumber(self.height.load(Ordering::Relaxed)) })
    }

    async fn get_proposal_content(
        &self,
        input: GetProposalContentInput,
    ) -> BatcherClientResult<GetProposalContentResponse> {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        Ok(GetProposalContentResponse { content: GetProposalContent::Finished {
            id: Default::default(),
            final_n_executed_txs: 0,
        }})
    }

    async fn validate_block(&self, input: ValidateBlockInput) -> BatcherClientResult<()> {
        Ok(())
    }

    async fn send_proposal_content(
        &self,
        input: SendProposalContentInput,
    ) -> BatcherClientResult<SendProposalContentResponse> {
        match input.content {
            SendProposalContent::Txs(_) => {
                Ok(SendProposalContentResponse { response: ProposalStatus::Processing })
            }
            SendProposalContent::Finish(_) => Ok(SendProposalContentResponse {
                response: ProposalStatus::Finished(Default::default()),
            }),
            SendProposalContent::Abort => {
                Ok(SendProposalContentResponse { response: ProposalStatus::Aborted })
            }
        }
    }

    async fn start_height(&self, input: StartHeightInput) -> BatcherClientResult<()> {
        self.height.store(input.height.0, Ordering::Relaxed);
        Ok(())
    }

    async fn add_sync_block(&self, sync_block: SyncBlock) -> BatcherClientResult<()> {
        Ok(())
    }

    async fn decision_reached(
        &self,
        input: DecisionReachedInput,
    ) -> BatcherClientResult<DecisionReachedResponse> {
        Ok(DecisionReachedResponse {
            state_diff: Default::default(),
            l2_gas_used: Default::default(),
            central_objects: CentralObjects {
                execution_infos: Default::default(),
                bouncer_weights: Default::default(),
                compressed_state_diff: None,
                casm_hash_computation_data_sierra_gas: Default::default(),
                casm_hash_computation_data_proving_gas: Default::default(),
            },
        })
    }

    async fn revert_block(&self, input: RevertBlockInput) -> BatcherClientResult<()> {
        self.height.compare_exchange(
            input.height.0,
            input.height.0 - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ).unwrap();
        Ok(())
    }
}

impl ConsensusManager {
    pub fn new(
        config: ConsensusManagerConfig,
        batcher_client: SharedBatcherClient,
        state_sync_client: SharedStateSyncClient,
        class_manager_client: SharedClassManagerClient,
        l1_gas_price_provider: Arc<dyn L1GasPriceProviderClient>,
    ) -> Self {
        let batcher_client = Arc::new(MockBatcherClient { height: AtomicU64::new(0) });
        Self {
            config,
            batcher_client,
            state_sync_client,
            class_manager_client,
            l1_gas_price_provider,
        }
    }

    pub async fn run(&self) -> Result<(), ConsensusError> {
        // Sleep to let sync advance and not reach a race condition where sync returns None to
        // consensus even though the block exists in the network
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        let height =
            self.state_sync_client.get_latest_block_number().await.unwrap().unwrap_or_default();
        self.batcher_client.start_height(StartHeightInput { height }).await.unwrap();
        if self.config.revert_config.should_revert {
            self.revert_batcher_blocks(self.config.revert_config.revert_up_to_and_including).await;
        }

        let mut broadcast_metrics_by_topic = HashMap::new();
        broadcast_metrics_by_topic.insert(
            Topic::new(self.config.votes_topic.clone()).hash(),
            BroadcastNetworkMetrics {
                num_sent_broadcast_messages: CONSENSUS_VOTES_NUM_SENT_MESSAGES,
                num_received_broadcast_messages: CONSENSUS_VOTES_NUM_RECEIVED_MESSAGES,
            },
        );
        broadcast_metrics_by_topic.insert(
            Topic::new(self.config.proposals_topic.clone()).hash(),
            BroadcastNetworkMetrics {
                num_sent_broadcast_messages: CONSENSUS_PROPOSALS_NUM_SENT_MESSAGES,
                num_received_broadcast_messages: CONSENSUS_PROPOSALS_NUM_RECEIVED_MESSAGES,
            },
        );
        let network_manager_metrics = Some(NetworkMetrics {
            num_connected_peers: CONSENSUS_NUM_CONNECTED_PEERS,
            num_blacklisted_peers: CONSENSUS_NUM_BLACKLISTED_PEERS,
            broadcast_metrics_by_topic: Some(broadcast_metrics_by_topic),
            sqmr_metrics: None,
            event_metrics: None,
        });
        let mut network_manager =
            NetworkManager::new(self.config.network_config.clone(), None, network_manager_metrics);

        let proposals_broadcast_channels = network_manager
            .register_broadcast_topic::<StreamMessage<ProposalPart, HeightAndRound>>(
                Topic::new(self.config.proposals_topic.clone()),
                self.config.broadcast_buffer_size,
            )
            .expect("Failed to register broadcast topic");

        let votes_broadcast_channels = network_manager
            .register_broadcast_topic::<Vote>(
                Topic::new(self.config.votes_topic.clone()),
                self.config.broadcast_buffer_size,
            )
            .expect("Failed to register broadcast topic");

        let BroadcastTopicChannels {
            broadcasted_messages_receiver: inbound_network_receiver,
            broadcast_topic_client: outbound_network_sender,
        } = proposals_broadcast_channels;

        let (inbound_internal_sender, inbound_internal_receiver) =
            mpsc::channel(self.config.stream_handler_config.channel_buffer_capacity);
        let (outbound_internal_sender, outbound_internal_receiver) =
            mpsc::channel(self.config.stream_handler_config.channel_buffer_capacity);
        let stream_handler = StreamHandler::new(
            self.config.stream_handler_config.clone(),
            inbound_internal_sender,
            inbound_network_receiver,
            outbound_internal_receiver,
            outbound_network_sender,
        );

        let observer_height = self
            .batcher_client
            .get_height()
            .await
            .expect("Failed to get observer_height from batcher")
            .height;
        let active_height = if self.config.immediate_active_height == observer_height {
            // Setting `start_height` is only used to enable consensus starting immediately without
            // observing the first height. This means consensus may return to a height
            // it has already voted on, risking equivocation. This is only safe to do if we
            // restart all nodes at this height.
            observer_height
        } else {
            BlockNumber(observer_height.0 + 1)
        };

        let context = SequencerConsensusContext::new(
            self.config.context_config.clone(),
            SequencerConsensusContextDeps {
                transaction_converter: Arc::new(TransactionConverter::new(
                    Arc::clone(&self.class_manager_client),
                    self.config.context_config.chain_id.clone(),
                )),
                state_sync_client: Arc::clone(&self.state_sync_client),
                batcher: Arc::clone(&self.batcher_client),
                cende_ambassador: Arc::new(CendeAmbassador::new(
                    self.config.cende_config.clone(),
                    Arc::clone(&self.class_manager_client),
                )),
                l1_gas_price_provider: self.l1_gas_price_provider.clone(),
                clock: Arc::new(DefaultClock),
                outbound_proposal_sender: outbound_internal_sender,
                vote_broadcast_client: votes_broadcast_channels.broadcast_topic_client.clone(),
            },
        );

        let network_task =
            tokio::spawn(network_manager.run().instrument(info_span!("[Consensus network]")));
        let stream_handler_task = tokio::spawn(stream_handler.run());
        let quorum_type = if self.config.assume_no_malicious_validators {
            QuorumType::Honest
        } else {
            QuorumType::Byzantine
        };
        let run_consensus_args = apollo_consensus::RunConsensusArguments {
            start_active_height: active_height,
            start_observe_height: observer_height,
            validator_id: self.config.consensus_manager_config.validator_id,
            consensus_delay: self.config.consensus_manager_config.startup_delay,
            timeouts: self.config.consensus_manager_config.timeouts.clone(),
            sync_retry_interval: self.config.consensus_manager_config.sync_retry_interval,
            quorum_type,
        };
        let consensus_fut = apollo_consensus::run_consensus(
            run_consensus_args,
            context,
            votes_broadcast_channels.into(),
            inbound_internal_receiver,
        );

        tokio::select! {
            consensus_result = consensus_fut => {
                match consensus_result {
                    Ok(_) => panic!("Consensus task finished unexpectedly"),
                    Err(e) => Err(e),
                }
            },
            network_result = network_task => {
                panic!("Consensus' network task finished unexpectedly: {:?}", network_result);
            }
            stream_handler_result = stream_handler_task => {
                panic!("Consensus' stream handler task finished unexpectedly: {:?}", stream_handler_result);
            }
        }
    }

    // Performs reverts to the batcher.
    async fn revert_batcher_blocks(&self, revert_up_to_and_including: BlockNumber) {
        // If we revert all blocks up to height X (including), the new height marker will be X.
        let batcher_height_marker = self
            .batcher_client
            .get_height()
            .await
            .expect("Failed to get batcher_height_marker from batcher")
            .height;

        // This function will panic if the revert fails.
        let revert_blocks_fn = move |height| async move {
            self.batcher_client
                .revert_block(RevertBlockInput { height })
                .await
                .expect("Failed to revert block at height {height} in the batcher");
        };

        revert_blocks_and_eternal_pending(
            batcher_height_marker,
            revert_up_to_and_including,
            revert_blocks_fn,
            "Batcher",
        )
        .await;
    }
}

pub fn create_consensus_manager(
    config: ConsensusManagerConfig,
    batcher_client: SharedBatcherClient,
    state_sync_client: SharedStateSyncClient,
    class_manager_client: SharedClassManagerClient,
    l1_gas_price_provider: Arc<dyn L1GasPriceProviderClient>,
) -> ConsensusManager {
    ConsensusManager::new(
        config,
        batcher_client,
        state_sync_client,
        class_manager_client,
        l1_gas_price_provider,
    )
}

#[async_trait]
impl ComponentStarter for ConsensusManager {
    async fn start(&mut self) {
        info!("Starting component {}.", short_type_name::<Self>());
        self.run()
            .await
            .unwrap_or_else(|e| panic!("Failed to start ConsensusManager component: {:?}", e))
    }
}

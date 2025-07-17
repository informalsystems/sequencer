use std::sync::Arc;

use starknet_api::block::GasPrice;
use starknet_api::consensus_transaction::InternalConsensusTransaction;
use starknet_api::core::{ClassHash, CompiledClassHash, EntryPointSelector};
use starknet_api::data_availability::DataAvailabilityMode;
use starknet_api::executable_transaction::L1HandlerTransaction;
use starknet_api::execution_resources::GasAmount;
use starknet_api::rpc_transaction::{
    InternalRpcDeclareTransactionV3, InternalRpcDeployAccountTransaction, InternalRpcTransaction,
    InternalRpcTransactionWithoutTxHash, RpcDeployAccountTransaction,
    RpcDeployAccountTransactionV3, RpcInvokeTransaction, RpcInvokeTransactionV3,
};
use starknet_api::transaction::fields::{
    AccountDeploymentData, AllResourceBounds, Calldata, ContractAddressSalt, Fee, PaymasterData,
    ResourceBounds, Tip, TransactionSignature,
};
use starknet_api::transaction::{TransactionHash, TransactionVersion};
use starknet_api::{contract_address, felt, nonce};
use starknet_types_core::felt::Felt;

pub struct Prng(u64);

impl Prng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);

        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

pub struct GenTxs {
    /// XXX: Declare transactions are not supported for now
    pub declare_count: usize,
    pub invoke_count: usize,
    pub deploy_account_count: usize,
    pub l1_handler_count: usize,
}

#[allow(dead_code)]
impl GenTxs {
    pub fn small() -> Self {
        Self { declare_count: 0, invoke_count: 1, deploy_account_count: 1, l1_handler_count: 1 }
    }

    pub fn medium() -> Self {
        Self { declare_count: 0, invoke_count: 10, deploy_account_count: 10, l1_handler_count: 10 }
    }

    pub fn large() -> Self {
        Self {
            declare_count: 0,
            invoke_count: 100,
            deploy_account_count: 100,
            l1_handler_count: 100,
        }
    }

    pub fn gen(&self, rng: &mut Prng) -> Vec<InternalConsensusTransaction> {
        let declares: Vec<_> = (0..self.declare_count)
            .map(|_| {
                InternalConsensusTransaction::RpcTransaction(InternalRpcTransaction {
                    tx: InternalRpcTransactionWithoutTxHash::Declare(declare_transaction(
                        rng.next(),
                    )),
                    tx_hash: TransactionHash(felt!(rng.next())),
                })
            })
            .collect();

        let invokes: Vec<_> = (0..self.invoke_count)
            .map(|_| {
                InternalConsensusTransaction::RpcTransaction(InternalRpcTransaction {
                    tx: InternalRpcTransactionWithoutTxHash::Invoke(invoke_transaction(rng.next())),
                    tx_hash: TransactionHash(felt!(rng.next())),
                })
            })
            .collect();

        let deploy_accounts: Vec<_> = (0..self.deploy_account_count)
            .map(|_| {
                InternalConsensusTransaction::RpcTransaction(InternalRpcTransaction {
                    tx: InternalRpcTransactionWithoutTxHash::DeployAccount(deploy_account_tx(
                        rng.next(),
                    )),
                    tx_hash: TransactionHash(felt!(rng.next())),
                })
            })
            .collect();

        let l1_handlers: Vec<_> = (0..self.l1_handler_count)
            .map(|_| InternalConsensusTransaction::L1Handler(l1_handler_tx(rng.next())))
            .collect();

        let mut transactions = declares;
        transactions.extend(invokes);
        transactions.extend(deploy_accounts);
        transactions.extend(l1_handlers);
        transactions
    }
}

pub fn declare_transaction(seed: u64) -> InternalRpcDeclareTransactionV3 {
    InternalRpcDeclareTransactionV3 {
        resource_bounds: resource_bounds(),
        tip: Tip(1),
        signature: TransactionSignature(felt_vector()),
        nonce: nonce!(seed),
        class_hash: declare_class_hash(),
        compiled_class_hash: declare_compiled_class_hash(),
        sender_address: contract_address!("0x12fd537"),
        nonce_data_availability_mode: DataAvailabilityMode::L1,
        fee_data_availability_mode: DataAvailabilityMode::L1,
        paymaster_data: PaymasterData(vec![]),
        account_deployment_data: AccountDeploymentData(vec![]),
    }
}

pub fn resource_bounds() -> AllResourceBounds {
    AllResourceBounds {
        l1_gas: ResourceBounds { max_amount: GasAmount(1), max_price_per_unit: GasPrice(1) },
        l2_gas: ResourceBounds { max_amount: GasAmount(2), max_price_per_unit: GasPrice(2) },
        l1_data_gas: ResourceBounds { max_amount: GasAmount(3), max_price_per_unit: GasPrice(3) },
    }
}

pub fn felt_vector() -> Vec<Felt> {
    vec![felt!(0_u8), felt!(1_u8), felt!(2_u8)]
}

pub fn declare_class_hash() -> ClassHash {
    ClassHash(felt!("0x3a59046762823dc87385eb5ac8a21f3f5bfe4274151c6eb633737656c209056"))
}

pub fn declare_compiled_class_hash() -> CompiledClassHash {
    CompiledClassHash(felt!(1_u8))
}

fn invoke_transaction(seed: u64) -> RpcInvokeTransaction {
    RpcInvokeTransaction::V3(RpcInvokeTransactionV3 {
        resource_bounds: resource_bounds(),
        tip: Tip(1),
        signature: TransactionSignature(felt_vector()),
        nonce: nonce!(seed),
        sender_address: contract_address!(
            "0x14abfd58671a1a9b30de2fcd2a42e8bff2ce1096a7c70bc7995904965f277e"
        ),
        calldata: Calldata(Arc::new(vec![felt!(0_u8), felt!(1_u8)])),
        nonce_data_availability_mode: DataAvailabilityMode::L1,
        fee_data_availability_mode: DataAvailabilityMode::L1,
        paymaster_data: PaymasterData(vec![]),
        account_deployment_data: AccountDeploymentData(vec![]),
    })
}

fn deploy_account_tx(seed: u64) -> InternalRpcDeployAccountTransaction {
    InternalRpcDeployAccountTransaction {
        tx: RpcDeployAccountTransaction::V3(RpcDeployAccountTransactionV3 {
            resource_bounds: resource_bounds(),
            tip: Tip(1),
            signature: TransactionSignature(felt_vector()),
            nonce: nonce!(seed),
            class_hash: ClassHash(felt!(
                "0x1b5a0b09f23b091d5d1fa2f660ddfad6bcfce607deba23806cd7328ccfb8ee9"
            )),
            contract_address_salt: ContractAddressSalt(felt!(2_u8)),
            constructor_calldata: Calldata(Arc::new(felt_vector())),
            nonce_data_availability_mode: DataAvailabilityMode::L1,
            fee_data_availability_mode: DataAvailabilityMode::L1,
            paymaster_data: PaymasterData(vec![]),
        }),
        contract_address: contract_address!(
            "0x4c2e031b0ddaa38e06fd9b1bf32bff739965f9d64833006204c67cbc879a57c"
        ),
    }
}

fn l1_handler_tx(seed: u64) -> L1HandlerTransaction {
    L1HandlerTransaction {
        tx: starknet_api::transaction::L1HandlerTransaction {
            version: TransactionVersion::ZERO,
            nonce: nonce!(seed),
            contract_address: contract_address!(
                "0x14abfd58671a1a9b30de2fcd2a42e8bff2ce1096a7c70bc7995904965f277e"
            ),
            entry_point_selector: EntryPointSelector(felt!("0x2a")),
            calldata: Calldata(Arc::new(vec![felt!(0_u8), felt!(1_u8)])),
        },
        tx_hash: TransactionHash(felt!(seed)),
        paid_fee_on_l1: Fee(1),
    }
}

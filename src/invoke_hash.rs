use serde::Deserialize;
use serde_json::Value;
use starknet_core::{
    crypto::compute_hash_on_elements,
    types::{BroadcastedInvokeTransactionV3, DataAvailabilityMode, ResourceBounds},
};
use starknet_types_core::{
    felt::Felt,
    hash::{Poseidon, StarkHash},
};

const INVOKE: Felt = Felt::from_hex_unchecked("0x696e766f6b65");

// Starknet's transaction hash identifies the broadcasted INVOKE before C1 acknowledges it.
pub fn invoke_hash(transaction: &Value, chain_id: Felt) -> Option<String> {
    let version = Felt::deserialize(&transaction["version"]).ok()?;
    let hash = match version {
        version if version == Felt::ONE => {
            let sender = Felt::deserialize(&transaction["sender_address"]).ok()?;
            let calldata = Vec::<Felt>::deserialize(&transaction["calldata"]).ok()?;
            let max_fee = Felt::deserialize(&transaction["max_fee"]).ok()?;
            let nonce = Felt::deserialize(&transaction["nonce"]).ok()?;
            compute_hash_on_elements(&[
                INVOKE,
                version,
                sender,
                Felt::ZERO,
                compute_hash_on_elements(&calldata),
                max_fee,
                chain_id,
                nonce,
            ])
        }
        version if version == Felt::THREE => {
            let tx = BroadcastedInvokeTransactionV3::deserialize(transaction).ok()?;
            let fee = Poseidon::hash_array(&[
                Felt::from(tx.tip),
                resource(b"L1_GAS", &tx.resource_bounds.l1_gas),
                resource(b"L2_GAS", &tx.resource_bounds.l2_gas),
                resource(b"L1_DATA", &tx.resource_bounds.l1_data_gas),
            ]);
            let da = (u64::from(tx.nonce_data_availability_mode == DataAvailabilityMode::L2) << 32)
                | u64::from(tx.fee_data_availability_mode == DataAvailabilityMode::L2);
            Poseidon::hash_array(&[
                INVOKE,
                version,
                tx.sender_address,
                fee,
                Poseidon::hash_array(&tx.paymaster_data),
                chain_id,
                tx.nonce,
                Felt::from(da),
                Poseidon::hash_array(&tx.account_deployment_data),
                Poseidon::hash_array(&tx.calldata),
            ])
        }
        _ => return None,
    };
    Some(format!("{hash:#x}"))
}

fn resource(name: &[u8], bound: &ResourceBounds) -> Felt {
    let mut bytes = [0; 32];
    bytes[8 - name.len()..8].copy_from_slice(name);
    bytes[8..16].copy_from_slice(&bound.max_amount.to_be_bytes());
    bytes[16..].copy_from_slice(&bound.max_price_per_unit.to_be_bytes());
    Felt::from_bytes_be(&bytes)
}

//! Tests for upgrade safety validation (Issue: `upgrade()` allowed arbitrary
//! WASM with no compatibility checks).
//!
//! These tests exercise the manifest-based compatibility checks added in
//! `validate_upgrade_compatibility` / `upgrade_with_manifest`:
//!
//!   1. `validate_upgrade_compatibility` panics with `UpgradeManifestNotSet`
//!      when no baseline has been recorded yet.
//!   2. Once a baseline is recorded via `set_upgrade_manifest`, a proposed
//!      upgrade with a smaller exported function count is rejected
//!      (`UpgradeInterfaceShrunk`).
//!   3. A proposed upgrade with fewer error codes than the baseline is
//!      rejected (`UpgradeErrorCodesReduced`).
//!   4. A proposed upgrade with a different storage schema hash is rejected
//!      (`UpgradeStorageSchemaChanged`).
//!   5. A fully compatible proposed upgrade passes validation.
//!   6. `upgrade_with_manifest` records a new baseline (with incremented
//!      version) after a successful upgrade, so a subsequent upgrade is
//!      validated against the *new* contract, not the original one.

#![cfg(test)]

use super::*;
use soroban_sdk::{
    testutils::{storage::Instance as _, Address as _},
    token::StellarAssetClient,
    Address, BytesN, Env,
};

const UPGRADE_WASM: &[u8] =
    include_bytes!("../../../target/wasm32-unknown-unknown/release/ttl_vault.wasm");

fn setup() -> (Env, Address, TtlVaultContractClient<'static>) {
    let env = Env::default();
    env.mock_all_auths();

    let admin = Address::generate(&env);
    let token_admin = Address::generate(&env);
    let token_address = env
        .register_stellar_asset_contract_v2(token_admin)
        .address();
    StellarAssetClient::new(&env, &token_address);

    let contract_address = env.register_contract(None, TtlVaultContract);
    let client = TtlVaultContractClient::new(&env, &contract_address);
    client.initialize(&token_address, &admin);

    let client: TtlVaultContractClient<'static> = unsafe { core::mem::transmute(client) };
    (env, admin, client)
}

fn schema_hash(env: &Env, seed: u8) -> BytesN<32> {
    BytesN::from_array(env, &[seed; 32])
}

fn signature_hash(env: &Env, seed: u8) -> BytesN<32> {
    BytesN::from_array(env, &[seed; 32])
}

#[test]
fn validate_upgrade_compatibility_fails_without_manifest() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);

    let result = client.try_validate_upgrade_compatibility(&10u32, &5u32, &signatures, &schema);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(ContractError::UpgradeManifestNotSet as u32)
    );
}

#[test]
fn legacy_manifest_without_signature_baseline_fails_closed() {
    let (env, _admin, client) = setup();
    let legacy_manifest = UpgradeManifest {
        exported_fn_count: 10,
        error_code_count: 5,
        storage_schema_hash: schema_hash(&env, 1),
        version: 1,
    };
    env.as_contract(&client.address, || {
        env.storage()
            .instance()
            .set(&DataKey::UpgradeManifest, &legacy_manifest);
    });

    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);
    let result = client.try_validate_upgrade_compatibility(&10u32, &5u32, &signatures, &schema);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(
            ContractError::UpgradeFunctionSignaturesNotSet as u32
        )
    );
}

#[test]
fn set_upgrade_manifest_then_compatible_upgrade_passes() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);

    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);

    // Same interface, error codes, and storage schema are compatible.
    client.validate_upgrade_compatibility(&10u32, &5u32, &signatures, &schema);

    // Adding error codes is fine when existing signatures remain.
    client.validate_upgrade_compatibility(&10u32, &6u32, &signatures, &schema);
}

#[test]
fn shrinking_exported_fn_count_is_rejected() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);
    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);

    let result = client.try_validate_upgrade_compatibility(&9u32, &5u32, &signatures, &schema);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(ContractError::UpgradeInterfaceShrunk as u32)
    );
}

#[test]
fn reducing_error_code_count_is_rejected() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);
    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);

    let result = client.try_validate_upgrade_compatibility(&10u32, &4u32, &signatures, &schema);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(ContractError::UpgradeErrorCodesReduced as u32)
    );
}

#[test]
fn changing_an_exported_function_signature_is_rejected() {
    let (env, _admin, client) = setup();
    let baseline_signatures = signature_hash(&env, 1);
    let changed_signatures = signature_hash(&env, 2);
    let schema = schema_hash(&env, 1);
    client.set_upgrade_manifest(&10u32, &5u32, &baseline_signatures, &schema);

    let result =
        client.try_validate_upgrade_compatibility(&10u32, &5u32, &changed_signatures, &schema);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(
            ContractError::UpgradeFunctionSignaturesChanged as u32
        )
    );
}

#[test]
fn changing_storage_schema_hash_is_rejected() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let baseline_hash = schema_hash(&env, 1);
    let different_hash = schema_hash(&env, 2);
    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &baseline_hash);

    let result =
        client.try_validate_upgrade_compatibility(&10u32, &5u32, &signatures, &different_hash);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(ContractError::UpgradeStorageSchemaChanged as u32)
    );
}

#[test]
fn set_upgrade_manifest_increments_version() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);

    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);
    let first = client.get_upgrade_manifest().unwrap();
    assert_eq!(first.version, 1);

    client.set_upgrade_manifest(&11u32, &5u32, &signatures, &schema);
    let second = client.get_upgrade_manifest().unwrap();
    assert_eq!(second.version, 2);
    assert_eq!(second.exported_fn_count, 11);

    let _ = env;
}

#[test]
fn upgrade_with_manifest_rejects_invalid_hash() {
    let (env, _admin, client) = setup();
    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);
    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);

    let zero_hash = BytesN::from_array(&env, &[0u8; 32]);
    let result = client.try_upgrade_with_manifest(&zero_hash, &10u32, &5u32, &signatures, &schema);
    assert_eq!(
        result.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(ContractError::UpgradeInvalidHash as u32)
    );
}

#[test]
fn rejected_signature_upgrade_keeps_the_running_contract_and_state() {
    let (env, admin, client) = setup();
    let owner = Address::generate(&env);
    let beneficiary = Address::generate(&env);
    let token_address = client.get_contract_token();
    StellarAssetClient::new(&env, &token_address).mint(&owner, &500);
    let vault_id = client.create_vault(&owner, &beneficiary, &100u64, &None);
    client.deposit(&vault_id, &owner, &500i128);

    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);
    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);
    let changed_signatures = signature_hash(&env, 2);
    let rejected = client.try_upgrade_with_manifest(
        &changed_signatures,
        &10u32,
        &5u32,
        &changed_signatures,
        &schema,
    );
    assert_eq!(
        rejected.unwrap_err().unwrap(),
        soroban_sdk::Error::from_contract_error(
            ContractError::UpgradeFunctionSignaturesChanged as u32
        )
    );

    assert_eq!(client.get_admin(), admin);
    assert_eq!(client.vault_count(), 1);
    assert_eq!(client.get_vault_balance(&vault_id), 500);
    assert_eq!(client.get_vault_owner(&vault_id), owner);
    assert_eq!(client.get_upgrade_manifest().unwrap().version, 1);
}

#[test]
fn wasm_upgrade_preserves_state_and_exported_function_signatures() {
    let (env, admin, client) = setup();
    let owner = Address::generate(&env);
    let beneficiary = Address::generate(&env);
    let token_address = client.get_contract_token();
    StellarAssetClient::new(&env, &token_address).mint(&owner, &500);
    let vault_id = client.create_vault(&owner, &beneficiary, &100u64, &None);
    client.deposit(&vault_id, &owner, &500i128);

    let signatures = signature_hash(&env, 1);
    let schema = schema_hash(&env, 1);
    client.set_upgrade_manifest(&10u32, &5u32, &signatures, &schema);

    let wasm_hash = env.deployer().upload_contract_wasm(UPGRADE_WASM);
    client.upgrade_with_manifest(&wasm_hash, &10u32, &5u32, &signatures, &schema);

    assert_eq!(client.get_admin(), admin);
    assert_eq!(client.get_contract_token(), token_address);
    assert_eq!(client.vault_count(), 1);
    assert_eq!(client.get_vault_balance(&vault_id), 500);
    assert_eq!(client.get_vault_owner(&vault_id), owner);
    assert_eq!(client.get_upgrade_manifest().unwrap().version, 2);
}

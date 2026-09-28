# Upgrade Safety

`contracts/ttl_vault/src/lib.rs` exposes `upgrade(env, new_wasm_hash)`,
which lets the admin replace the deployed WASM with arbitrary new code.
Historically this only checked that the hash was non-zero — it did not
verify the new code was interface-compatible, storage-compatible, or
error-code-compatible with the running contract. This document describes
the compatibility checks that were added and how to use them.

## What is checked

Because a Soroban contract cannot introspect the raw bytes of a WASM blob
it hasn't deployed yet, compatibility is enforced via an **admin-recorded
manifest** (`UpgradeManifest` and its separate signature-hash key in
`types.rs`) rather than by disassembling the new WASM on-chain:

| Field | What it protects against |
|---|---|
| `exported_fn_count` | The new contract exporting fewer public functions than the running one (an accidentally-shrunk interface breaking existing integrators). |
| `exported_fn_signature_hash` | A changed or removed exported function signature, even when the total function count is unchanged. Compute this from the sorted canonical function names, argument types, and return types in the Soroban contract spec. |
| `error_code_count` | The new contract having fewer `ContractError` variants than the running one (error codes being removed or renumbered, which silently changes the meaning of an error an integrator is already handling). |
| `storage_schema_hash` | The new contract no longer reading/writing a storage key the running contract uses (a backward-incompatible storage migration). |

`validate_upgrade_compatibility` panics with a specific error
(`UpgradeInterfaceShrunk`, `UpgradeErrorCodesReduced`,
`UpgradeStorageSchemaChanged`, `UpgradeManifestNotSet`,
`UpgradeFunctionSignaturesNotSet`, or
`UpgradeFunctionSignaturesChanged`) when a proposed upgrade fails one of
these checks.

## Admin workflow

1. **After first deploying/initializing the contract**, record a baseline:

   ```
   set_upgrade_manifest(exported_fn_count, error_code_count, exported_fn_signature_hash, storage_schema_hash)
   ```

   Compute these values with an off-chain tool that inspects the
   deployed WASM (e.g. `wasm-objdump -x` for the export table count, a
   SHA-256 over sorted canonical signatures in its `contractspecv0` section,
   a count of `ContractError` variants from the source, and a hash — e.g.
   SHA-256 — over the sorted list of `DataKey` variant names/tags used by
   the contract).

2. **Before every upgrade**, compute the same four values for the *new*
   candidate WASM and call:

   ```
   validate_upgrade_compatibility(new_exported_fn_count, new_error_code_count, new_exported_fn_signature_hash, new_storage_schema_hash)
   ```

   This can be simulated read-only before submitting the real upgrade
   transaction, to fail fast without spending an upgrade attempt.

3. **Perform the upgrade** with the same four values via:

   ```
   upgrade_with_manifest(new_wasm_hash, new_exported_fn_count, new_error_code_count, new_exported_fn_signature_hash, new_storage_schema_hash)
   ```

   This validates the hash (`validate_upgrade`), validates compatibility
   (`validate_upgrade_compatibility`), performs the WASM swap, and then
   records the new values as the baseline for the *next* upgrade
   (incrementing `UpgradeManifest.version`).

The legacy `upgrade(new_wasm_hash)` entry point still exists for
compatibility with existing tooling, but only performs the non-zero-hash
check — it does not consult the manifest. New deployments and operational
tooling should prefer `upgrade_with_manifest`.

## Limitations

- The manifest values are admin-declared, not independently verified
  on-chain — this is a guardrail against accidental interface/storage
  regressions, not a substitute for code review of the new WASM before an
  upgrade is proposed. The signature hash must be computed from canonical
  Soroban contract-spec function entries, sorted by function name; include
  each argument name and type and the return type so signature changes alter
  the hash. The manifest currently requires the exported signature hash to
  match exactly; interface additions therefore require an explicitly reviewed
  manifest update. The signature hash is stored separately from the original
  `UpgradeManifest` record to preserve compatibility with deployments that
  already have a manifest. For those deployments, call `set_upgrade_manifest`
  once more to record the new signature baseline before attempting an upgrade;
  until then, compatibility validation fails closed with
  `UpgradeFunctionSignaturesNotSet`.
- `storage_schema_hash` should be computed by hashing the *sorted* list of
  key names/tags used by the contract (available in `types.rs`'s `DataKey`
  enum) so that reordering the enum's declaration doesn't spuriously
  change the hash. Removing or renaming a variant, however, should.
- If an admin key is compromised, these checks do not prevent a malicious
  upgrade — an attacker with admin authority can compute a manifest for
  their own malicious contract and pass it in. Admin key security (e.g.
  the timelocked admin transfer flow already in this contract) remains the
  primary control. See `docs/runbook-alerts.md#ethoscontractupgradeinprogress`
  for the operational response when an unexpected upgrade is observed.

## Tests

CI builds the contract WASM before running `upgrade_validation_tests.rs`.
The Soroban local test environment uploads that artifact and swaps a registered
contract to it, then checks that admin, token, vault state, balances, and
exported-method calls still work. It also exercises rejected upgrades and
signature-hash regressions; no network or live-ledger upgrade is involved.

See `contracts/ttl_vault/src/upgrade_validation_tests.rs` for coverage of:
missing manifest, interface shrinkage, error code reduction, storage
schema drift, changed function signatures, rejected upgrade state safety,
actual WASM replacement, persistent state preservation, and manifest version
incrementing.

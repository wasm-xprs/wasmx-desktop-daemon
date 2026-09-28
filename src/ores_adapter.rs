use anyhow::{Result, bail};
use serde::Deserialize;
use std::path::{Component, Path};

pub const ORES_LAMBDA_ADAPTER_SCHEMA: &str = "ores.lambda.adapter/v1";
pub const ORES_GENERATOR: &str = "ores-stack";
pub const WASMX_PROVIDER: &str = "wasm_xprs";
pub const WASMX_RUNTIME_REPOSITORY: &str = "wasm-xprs/wasmx-lambdas";
pub const WASMX_RUNTIME_CONTRACT: &str = "wasm-xprs.lambda-runtime/v1";
pub const WASMX_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";
pub const WASMX_GUEST_ABI: &str = "wasmx-v1";
pub const WASMX_EXECUTION_BOUNDARY: &str = "wasmtime_store_instance";
pub const WASMX_ISOLATION_MODEL: &str = "fresh_store_and_instance_per_invocation";
pub const WASMX_ARTIFACT_KIND: &str = "wasm_module";
pub const WASMX_MODULE_CACHE_POLICY: &str = "compiled_module_allowed";
pub const WASMX_INSTANCE_REUSE: &str = "forbidden";
pub const WASMX_AMBIENT_IMPORT_POLICY: &str = "explicit_wasmx_v1_only";
pub const WASMX_DURABLE_STATE: &str = "external_only";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OresLambdaAdapterV1 {
    schema_version: String,
    generated_by: String,
    provider: String,
    runtime_repository: String,
    runtime_contract: String,
    execution_boundary: String,
    isolation_model: String,
    artifact_kind: String,
    module_cache_policy: String,
    invocation_instance_reuse: String,
    ambient_import_policy: String,
    durable_state: String,
    source: String,
    source_sha256: String,
}

impl OresLambdaAdapterV1 {
    pub fn validate(&self) -> Result<()> {
        require_eq(
            "schema_version",
            &self.schema_version,
            ORES_LAMBDA_ADAPTER_SCHEMA,
        )?;
        require_eq("generated_by", &self.generated_by, ORES_GENERATOR)?;
        require_eq("provider", &self.provider, WASMX_PROVIDER)?;
        require_eq(
            "runtime_repository",
            &self.runtime_repository,
            WASMX_RUNTIME_REPOSITORY,
        )?;
        require_eq(
            "runtime_contract",
            &self.runtime_contract,
            WASMX_RUNTIME_CONTRACT,
        )?;
        require_eq(
            "execution_boundary",
            &self.execution_boundary,
            WASMX_EXECUTION_BOUNDARY,
        )?;
        require_eq(
            "isolation_model",
            &self.isolation_model,
            WASMX_ISOLATION_MODEL,
        )?;
        require_eq("artifact_kind", &self.artifact_kind, WASMX_ARTIFACT_KIND)?;
        require_eq(
            "module_cache_policy",
            &self.module_cache_policy,
            WASMX_MODULE_CACHE_POLICY,
        )?;
        require_eq(
            "invocation_instance_reuse",
            &self.invocation_instance_reuse,
            WASMX_INSTANCE_REUSE,
        )?;
        require_eq(
            "ambient_import_policy",
            &self.ambient_import_policy,
            WASMX_AMBIENT_IMPORT_POLICY,
        )?;
        require_eq("durable_state", &self.durable_state, WASMX_DURABLE_STATE)?;
        validate_source_path(&self.source)?;
        validate_sha256(&self.source_sha256)?;
        return Ok(());
    }
}

fn require_eq(name: &str, actual: &str, expected: &str) -> Result<()> {
    if actual != expected {
        bail!("ORES adapter {name} must be {expected:?}, found {actual:?}");
    }
    return Ok(());
}

fn validate_source_path(value: &str) -> Result<()> {
    let path = Path::new(value);
    let valid = !value.is_empty()
        && !value.contains('\0')
        && !value.contains('\\')
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if !valid {
        bail!("ORES adapter source must be a normalized repository-relative path");
    }
    return Ok(());
}

fn validate_sha256(value: &str) -> Result<()> {
    let valid = value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    if !valid {
        bail!("ORES adapter source_sha256 must be 64 lowercase hexadecimal characters");
    }
    return Ok(());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_adapter() -> OresLambdaAdapterV1 {
        return OresLambdaAdapterV1 {
            schema_version: ORES_LAMBDA_ADAPTER_SCHEMA.to_owned(),
            generated_by: ORES_GENERATOR.to_owned(),
            provider: WASMX_PROVIDER.to_owned(),
            runtime_repository: WASMX_RUNTIME_REPOSITORY.to_owned(),
            runtime_contract: WASMX_RUNTIME_CONTRACT.to_owned(),
            execution_boundary: WASMX_EXECUTION_BOUNDARY.to_owned(),
            isolation_model: WASMX_ISOLATION_MODEL.to_owned(),
            artifact_kind: WASMX_ARTIFACT_KIND.to_owned(),
            module_cache_policy: WASMX_MODULE_CACHE_POLICY.to_owned(),
            invocation_instance_reuse: WASMX_INSTANCE_REUSE.to_owned(),
            ambient_import_policy: WASMX_AMBIENT_IMPORT_POLICY.to_owned(),
            durable_state: WASMX_DURABLE_STATE.to_owned(),
            source: "src/routes/echo/lambda.rs".to_owned(),
            source_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        };
    }

    #[test]
    fn exact_current_ores_adapter_is_admitted() -> Result<()> {
        return valid_adapter().validate();
    }

    #[test]
    fn widened_authority_or_reuse_fails_closed() {
        let mut adapter = valid_adapter();
        adapter.ambient_import_policy = "wasi".to_owned();
        assert!(adapter.validate().is_err());

        let mut adapter = valid_adapter();
        adapter.invocation_instance_reuse = "allowed".to_owned();
        assert!(adapter.validate().is_err());
    }

    #[test]
    fn path_and_digest_are_validated() {
        let mut adapter = valid_adapter();
        adapter.source = "../lambda.rs".to_owned();
        assert!(adapter.validate().is_err());

        let mut adapter = valid_adapter();
        adapter.source = "generated/provider-neutral.rs".to_owned();
        assert!(adapter.validate().is_ok());

        let mut adapter = valid_adapter();
        adapter.source_sha256 = "ABC".to_owned();
        assert!(adapter.validate().is_err());
    }
}

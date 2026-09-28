use anyhow::{Result, bail};
use serde::Deserialize;
use std::path::{Component, Path};

pub const ORES_LAMBDA_ADAPTER_SCHEMA: &str = "ores.lambda.adapter/v1";
pub const ORES_GENERATOR: &str = "ores-stack";
pub const WASMX_PROVIDER: &str = "wasm_xprs";
pub const WASMX_RUNTIME_STACK: &str = "wasm_xprs";
pub const WASMX_EXECUTION_MODEL: &str = "wasm_isolate";
pub const WASMX_ISOLATION_BOUNDARY: &str = "wasmtime_store";
pub const WASMX_ARTIFACT_FORMAT: &str = "wasm_module";
pub const WASMX_TARGET_TRIPLE: &str = "wasm32-unknown-unknown";
pub const WASMX_GUEST_ABI: &str = "wasmx-v1";
pub const WASMX_RUNTIME_REPOSITORY: &str = "https://github.com/wasm-xprs";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OresLambdaAdapterV1 {
    schema_version: String,
    generated_by: String,
    provider: String,
    runtime_stack: String,
    module_kind: String,
    execution_model: String,
    isolation_boundary: String,
    artifact_format: String,
    target_triple: String,
    guest_abi: String,
    wasi_enabled: bool,
    runtime_repository: String,
    actor_model: bool,
    multi_tenant_same_process: bool,
    source_path: String,
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
        require_eq("runtime_stack", &self.runtime_stack, WASMX_RUNTIME_STACK)?;
        require_eq("module_kind", &self.module_kind, "lambda")?;
        require_eq(
            "execution_model",
            &self.execution_model,
            WASMX_EXECUTION_MODEL,
        )?;
        require_eq(
            "isolation_boundary",
            &self.isolation_boundary,
            WASMX_ISOLATION_BOUNDARY,
        )?;
        require_eq(
            "artifact_format",
            &self.artifact_format,
            WASMX_ARTIFACT_FORMAT,
        )?;
        require_eq("target_triple", &self.target_triple, WASMX_TARGET_TRIPLE)?;
        require_eq("guest_abi", &self.guest_abi, WASMX_GUEST_ABI)?;
        if self.wasi_enabled {
            bail!("ORES adapter must declare wasi_enabled=false for wasm-xprs");
        }
        require_eq(
            "runtime_repository",
            &self.runtime_repository,
            WASMX_RUNTIME_REPOSITORY,
        )?;
        if self.actor_model {
            bail!("ORES adapter must declare actor_model=false for direct Wasmtime lambdas");
        }
        if !self.multi_tenant_same_process {
            bail!("ORES adapter must declare multi_tenant_same_process=true for wasm-xprs");
        }
        validate_source_path(&self.source_path)?;
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
        && value.ends_with("lambda.rs")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if !valid {
        bail!("ORES adapter source_path must be a normalized repository-relative lambda.rs path");
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
            runtime_stack: WASMX_RUNTIME_STACK.to_owned(),
            module_kind: "lambda".to_owned(),
            execution_model: WASMX_EXECUTION_MODEL.to_owned(),
            isolation_boundary: WASMX_ISOLATION_BOUNDARY.to_owned(),
            artifact_format: WASMX_ARTIFACT_FORMAT.to_owned(),
            target_triple: WASMX_TARGET_TRIPLE.to_owned(),
            guest_abi: WASMX_GUEST_ABI.to_owned(),
            wasi_enabled: false,
            runtime_repository: WASMX_RUNTIME_REPOSITORY.to_owned(),
            actor_model: false,
            multi_tenant_same_process: true,
            source_path: "src/routes/echo/lambda.rs".to_owned(),
            source_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        };
    }

    #[test]
    fn exact_wasmx_adapter_is_admitted() -> Result<()> {
        return valid_adapter().validate();
    }

    #[test]
    fn wasi_or_wrong_target_fails_closed() {
        let mut adapter = valid_adapter();
        adapter.wasi_enabled = true;
        assert!(adapter.validate().is_err());

        let mut adapter = valid_adapter();
        adapter.target_triple = "wasm32-wasip1".to_owned();
        assert!(adapter.validate().is_err());
    }

    #[test]
    fn path_and_digest_are_validated() {
        let mut adapter = valid_adapter();
        adapter.source_path = "../lambda.rs".to_owned();
        assert!(adapter.validate().is_err());

        let mut adapter = valid_adapter();
        adapter.source_sha256 = "ABC".to_owned();
        assert!(adapter.validate().is_err());
    }
}

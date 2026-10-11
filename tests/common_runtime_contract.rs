use ores_common_desktop_infra::runtime::{
    DesktopRuntimeAdapter, ExecutionTargetKind, ResourcePolicy, RuntimeCapabilities, RuntimeKind,
    RuntimeTargetSpec,
};

const SHA256: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

struct WasmxRuntimeContract;

impl DesktopRuntimeAdapter for WasmxRuntimeContract {
    fn product_id(&self) -> &str {
        return "wasm-xprs";
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        return RuntimeCapabilities {
            runtime_kind: RuntimeKind::Wasmtime,
            supports_standalone_server: true,
            supports_worker_pool: true,
            supports_actors: true,
            supports_hot_worker_reload: true,
            supports_hot_middleware_reload: true,
            supports_generation_drain: true,
        };
    }

    fn stage_target(&self, _target: &RuntimeTargetSpec) -> Result<(), String> {
        return Ok(());
    }

    fn start_target(&self, _target: &RuntimeTargetSpec, _generation: u64) -> Result<(), String> {
        return Ok(());
    }

    fn health_check_target(
        &self,
        _target: &RuntimeTargetSpec,
        _generation: u64,
    ) -> Result<(), String> {
        return Ok(());
    }

    fn begin_drain(&self, _generation: u64) -> Result<(), String> {
        return Ok(());
    }

    fn stop_generation(&self, _generation: u64) -> Result<(), String> {
        return Ok(());
    }

    fn rollback_generation(
        &self,
        _from_generation: u64,
        _to_generation: u64,
    ) -> Result<(), String> {
        return Ok(());
    }
}

fn worker_target() -> RuntimeTargetSpec {
    return RuntimeTargetSpec {
        target_id: "wasmx-worker-pool".to_string(),
        target_kind: ExecutionTargetKind::WorkerPool,
        immutable_revision: "0123456789abcdef0123456789abcdef01234567".to_string(),
        content_sha256: SHA256.to_string(),
        resource_policy: ResourcePolicy {
            memory_limit_bytes: Some(128 * 1024 * 1024),
            cpu_millis: Some(1_000),
            max_concurrency: Some(8),
            request_timeout_ms: Some(20 * 60 * 1_000),
            idle_timeout_ms: Some(60_000),
            max_instances: Some(8),
            min_instances: Some(1),
            max_queue_depth: Some(128),
        },
    };
}

#[test]
fn wasmx_declares_the_shared_wasmtime_runtime_capabilities() {
    let runtime = WasmxRuntimeContract;
    let capabilities = runtime.capabilities();

    assert_eq!(runtime.product_id(), "wasm-xprs");
    assert_eq!(capabilities.runtime_kind, RuntimeKind::Wasmtime);
    assert!(capabilities.supports_standalone_server);
    assert!(capabilities.supports_worker_pool);
    assert!(capabilities.supports_actors);
    assert!(capabilities.supports_hot_middleware_reload);
    assert!(capabilities.supports_generation_drain);
}

#[test]
fn wasmx_worker_resource_policy_is_valid_under_the_common_contract() {
    let target = worker_target();

    assert!(target.resource_policy.validate().is_ok());
}

#[test]
fn common_runtime_lifecycle_accepts_a_wasmx_worker_target() {
    let runtime = WasmxRuntimeContract;
    let target = worker_target();

    assert!(runtime.stage_target(&target).is_ok());
    assert!(runtime.start_target(&target, 1).is_ok());
    assert!(runtime.health_check_target(&target, 1).is_ok());
    assert!(runtime.begin_drain(1).is_ok());
    assert!(runtime.stop_generation(1).is_ok());
}

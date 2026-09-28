mod ores_adapter;

use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ores_adapter::{OresLambdaAdapterV1, WASMX_GUEST_ABI, WASMX_TARGET_TRIPLE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::{RwLock, Semaphore};
use uuid::Uuid;
use wasmtime::{
    Caller, Config, Engine, Extern, ExternType, FuncType, Linker, Module, Store, StoreLimits,
    StoreLimitsBuilder, ValType,
};

const DEFAULT_ADDR: &str = "127.0.0.1:8765";
const DEFAULT_MEMORY_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_PARALLEL: usize = 8;
const DEFAULT_MAX_PARALLEL_COMPILES: usize = 2;
const DEFAULT_MAX_CACHED_MODULES: usize = 128;
const DEFAULT_MAX_TENANT_DEPLOYMENTS: usize = 64;
const DEFAULT_MAX_TENANT_STORAGE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_FUEL: u64 = 50_000_000;
const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_FUEL: u64 = 500_000_000;
const MAX_MODULE_BYTES: usize = 64 * 1024 * 1024;
const MAX_IO_BYTES: usize = 10 * 1024 * 1024;
const MAX_HOSTCALL_BYTES: usize = 64 * 1024;
const MAX_LOG_BYTES: usize = 64 * 1024;
const EPOCH_TICK_MS: u64 = 10;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeploymentKey {
    tenant_id: String,
    deployment_id: String,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    artifact_root: Arc<PathBuf>,
    engine: Engine,
    modules: Arc<RwLock<HashMap<DeploymentKey, Module>>>,
    permits: Arc<Semaphore>,
    compile_permits: Arc<Semaphore>,
    max_cached_modules: usize,
    max_tenant_deployments: usize,
    max_tenant_storage_bytes: u64,
    max_memory_bytes: usize,
    default_fuel: u64,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeployRequest {
    tenant_id: String,
    deployment_id: String,
    wasm_base64: String,
    #[serde(default)]
    ores_adapter: Option<OresLambdaAdapterV1>,
}

#[derive(Debug, Serialize)]
struct DeployResponse {
    tenant_id: String,
    deployment_id: String,
    sha256: String,
    module_bytes: usize,
    compiled: bool,
    ores_adapter_verified: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeploymentManifest {
    schema_version: String,
    tenant_id: String,
    deployment_id: String,
    sha256: String,
    module_bytes: u64,
    guest_abi: String,
    target_triple: String,
    wasi_enabled: bool,
    ores_adapter_verified: bool,
}

#[derive(Debug, Serialize)]
struct DeploymentSummary {
    tenant_id: String,
    deployment_id: String,
    sha256: String,
    module_bytes: u64,
    cached: bool,
    integrity_verified: bool,
    ores_adapter_verified: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRequest {
    invocation_id: String,
    tenant_id: String,
    deployment_id: String,
    payload_json: Value,
    timeout_ms: Option<u64>,
    fuel: Option<u64>,
}

#[derive(Debug, Serialize)]
struct InvocationResponse {
    invocation_id: String,
    deployment_id: String,
    ok: bool,
    payload_json: Option<Value>,
    error: Option<String>,
    fuel_consumed: Option<u64>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    version: &'static str,
    runtime: &'static str,
    isolation: &'static str,
    guest_abi: &'static str,
    target_triple: &'static str,
    wasi_enabled: bool,
    store_per_invocation: bool,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    available_slots: usize,
    available_compile_slots: usize,
    cached_modules: usize,
    max_cached_modules: usize,
    max_tenant_deployments: usize,
    max_tenant_storage_bytes: u64,
    max_memory_bytes: usize,
    default_fuel: u64,
}

#[derive(Debug)]
struct HostState {
    input: Vec<u8>,
    output: Vec<u8>,
    log_bytes: usize,
    limits: StoreLimits,
}

#[derive(Debug)]
struct ExecutionResult {
    output: Vec<u8>,
    fuel_consumed: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "wasmx_desktop_daemon=info".into()),
        )
        .init();

    let addr = parse_loopback_addr(
        env::var("WASMX_DESKTOP_ADDR")
            .as_deref()
            .unwrap_or(DEFAULT_ADDR),
    )?;
    let token = load_or_create_token(&token_path()?)?;
    let artifact_root = artifact_root()?;
    std::fs::create_dir_all(&artifact_root)?;
    let max_memory_bytes = positive_usize_env("WASMX_MAX_MEMORY_BYTES", DEFAULT_MEMORY_BYTES)?;
    let max_parallel = positive_usize_env("WASMX_MAX_PARALLEL_INVOCATIONS", DEFAULT_MAX_PARALLEL)?;
    let max_parallel_compiles =
        positive_usize_env("WASMX_MAX_PARALLEL_COMPILES", DEFAULT_MAX_PARALLEL_COMPILES)?;
    let max_cached_modules =
        positive_usize_env("WASMX_MAX_CACHED_MODULES", DEFAULT_MAX_CACHED_MODULES)?;
    let max_tenant_deployments = positive_usize_env(
        "WASMX_MAX_TENANT_DEPLOYMENTS",
        DEFAULT_MAX_TENANT_DEPLOYMENTS,
    )?;
    let max_tenant_storage_bytes = positive_u64_env(
        "WASMX_MAX_TENANT_STORAGE_BYTES",
        DEFAULT_MAX_TENANT_STORAGE_BYTES,
    )?;
    let default_fuel = positive_u64_env("WASMX_DEFAULT_FUEL", DEFAULT_FUEL)?;

    let mut config = Config::new();
    config.consume_fuel(true);
    config.epoch_interruption(true);
    config.wasm_threads(false);
    config.wasm_memory64(false);
    let engine = Engine::new(&config)?;

    let state = AppState {
        token: Arc::from(token),
        artifact_root: Arc::new(artifact_root),
        engine: engine.clone(),
        modules: Arc::new(RwLock::new(HashMap::new())),
        permits: Arc::new(Semaphore::new(max_parallel)),
        compile_permits: Arc::new(Semaphore::new(max_parallel_compiles)),
        max_cached_modules,
        max_tenant_deployments,
        max_tenant_storage_bytes,
        max_memory_bytes,
        default_fuel,
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
    };

    tokio::spawn(epoch_ticker(engine));

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/v1/status", get(status))
        .route(
            "/v1/deploy",
            post(deploy).layer(DefaultBodyLimit::max(MAX_MODULE_BYTES * 2)),
        )
        .route("/v1/deployments", get(list_deployments))
        .route(
            "/v1/deployments/{tenant_id}/{deployment_id}",
            get(get_deployment).delete(delete_deployment),
        )
        .route(
            "/v1/invoke",
            post(invoke).layer(DefaultBodyLimit::max(MAX_IO_BYTES * 2)),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "wasm-xprs desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    return Ok(());
}

async fn health() -> &'static str {
    return "ok";
}

async fn ready(
    State(state): State<AppState>,
) -> Result<&'static str, (StatusCode, &'static str)> {
    match tokio::fs::metadata(state.artifact_root.as_ref()).await {
        Ok(metadata) if metadata.is_dir() => Ok("ready"),
        _ => Err((StatusCode::SERVICE_UNAVAILABLE, "not ready")),
    }
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let cached_modules = state.modules.read().await.len();
    return Ok(Json(StatusResponse {
        version: env!("CARGO_PKG_VERSION"),
        runtime: "wasmtime",
        isolation: "fresh_store_per_invocation",
        guest_abi: WASMX_GUEST_ABI,
        target_triple: WASMX_TARGET_TRIPLE,
        wasi_enabled: false,
        store_per_invocation: true,
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        available_slots: state.permits.available_permits(),
        available_compile_slots: state.compile_permits.available_permits(),
        cached_modules,
        max_cached_modules: state.max_cached_modules,
        max_tenant_deployments: state.max_tenant_deployments,
        max_tenant_storage_bytes: state.max_tenant_storage_bytes,
        max_memory_bytes: state.max_memory_bytes,
        default_fuel: state.default_fuel,
    }));
}

async fn deploy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DeployRequest>,
) -> Result<Json<DeployResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &request.tenant_id)?;
    validate_identifier("deployment_id", &request.deployment_id)?;

    let ores_adapter_verified = match request.ores_adapter.as_ref() {
        Some(adapter) => {
            adapter.validate().map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("ORES adapter validation failed: {error}"),
                )
            })?;
            true
        }
        None => false,
    };

    let bytes = BASE64.decode(request.wasm_base64.as_bytes()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "wasm_base64 is not valid base64".to_owned(),
        )
    })?;
    if bytes.is_empty() || bytes.len() > MAX_MODULE_BYTES {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("module must be between 1 and {MAX_MODULE_BYTES} bytes"),
        ));
    }

    let module = compile_module(&state, bytes.clone())
        .await
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("module validation failed: {error}"),
            )
        })?;

    let key = DeploymentKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let path = artifact_path(&state.artifact_root, &key).map_err(internal_error)?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));

    if tokio::fs::try_exists(&path).await.map_err(internal_error)? {
        let existing = tokio::fs::read(&path).await.map_err(internal_error)?;
        if existing != bytes {
            return Err((
                StatusCode::CONFLICT,
                "deployment_id is immutable and already contains a different module".to_owned(),
            ));
        }
    } else {
        enforce_tenant_quota(&state, &key.tenant_id, bytes.len() as u64)
            .await
            .map_err(|error| (StatusCode::PAYLOAD_TOO_LARGE, error.to_string()))?;
        atomic_write(&path, &bytes).await.map_err(internal_error)?;
    }

    let manifest = DeploymentManifest {
        schema_version: "wasmx.deployment/v1".to_owned(),
        tenant_id: key.tenant_id.clone(),
        deployment_id: key.deployment_id.clone(),
        sha256: sha256.clone(),
        module_bytes: bytes.len() as u64,
        guest_abi: WASMX_GUEST_ABI.to_owned(),
        target_triple: WASMX_TARGET_TRIPLE.to_owned(),
        wasi_enabled: false,
        ores_adapter_verified,
    };
    write_manifest(&state.artifact_root, &key, &manifest)
        .await
        .map_err(internal_error)?;
    cache_module(&state, key, module).await;

    return Ok(Json(DeployResponse {
        tenant_id: request.tenant_id,
        deployment_id: request.deployment_id,
        sha256,
        module_bytes: bytes.len(),
        compiled: true,
        ores_adapter_verified,
    }));
}

async fn list_deployments(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<DeploymentSummary>>, (StatusCode, String)> {
    authorize(&headers, &state)?;

    let cached = state
        .modules
        .read()
        .await
        .keys()
        .cloned()
        .collect::<HashSet<_>>();

    let mut deployments = Vec::new();
    let mut tenants = tokio::fs::read_dir(state.artifact_root.as_ref())
        .await
        .map_err(internal_error)?;

    while let Some(tenant_entry) = tenants.next_entry().await.map_err(internal_error)? {
        if !tenant_entry
            .file_type()
            .await
            .map_err(internal_error)?
            .is_dir()
        {
            continue;
        }
        let tenant_id = tenant_entry.file_name().to_string_lossy().into_owned();
        if validate_path_component(&tenant_id).is_err() {
            continue;
        }

        let mut tenant_deployments = tokio::fs::read_dir(tenant_entry.path())
            .await
            .map_err(internal_error)?;
        while let Some(deployment_entry) = tenant_deployments
            .next_entry()
            .await
            .map_err(internal_error)?
        {
            if !deployment_entry
                .file_type()
                .await
                .map_err(internal_error)?
                .is_dir()
            {
                continue;
            }
            let deployment_id = deployment_entry.file_name().to_string_lossy().into_owned();
            if validate_path_component(&deployment_id).is_err() {
                continue;
            }

            let key = DeploymentKey {
                tenant_id: tenant_id.clone(),
                deployment_id: deployment_id.clone(),
            };
            let summary = deployment_summary(&state, &key, cached.contains(&key))
                .await
                .map_err(internal_error)?;
            deployments.push(summary);
        }
    }

    deployments
        .sort_by(|a, b| (&a.tenant_id, &a.deployment_id).cmp(&(&b.tenant_id, &b.deployment_id)));
    Ok(Json(deployments))
}

async fn get_deployment(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id)): AxumPath<(String, String)>,
) -> Result<Json<DeploymentSummary>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let key = DeploymentKey {
        tenant_id,
        deployment_id,
    };
    let cached = state.modules.read().await.contains_key(&key);
    let summary = deployment_summary(&state, &key, cached)
        .await
        .map_err(|error| {
            if error.to_string().contains("not found") {
                (StatusCode::NOT_FOUND, "deployment not found".to_owned())
            } else {
                internal_error(error)
            }
        })?;
    Ok(Json(summary))
}

async fn delete_deployment(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id)): AxumPath<(String, String)>,
) -> Result<Response<Body>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let key = DeploymentKey {
        tenant_id,
        deployment_id,
    };
    state.modules.write().await.remove(&key);
    let dir = artifact_dir(&state.artifact_root, &key).map_err(internal_error)?;
    return match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .map_err(internal_error),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "deployment not found".to_owned()))
        }
        Err(error) => Err(internal_error(error)),
    };
}

async fn invoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InvocationRequest>,
) -> Result<Json<InvocationResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("invocation_id", &request.invocation_id)?;
    validate_identifier("tenant_id", &request.tenant_id)?;
    validate_identifier("deployment_id", &request.deployment_id)?;

    let timeout_ms = request.timeout_ms.unwrap_or(30_000);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("timeout_ms must be between 1 and {MAX_TIMEOUT_MS}"),
        ));
    }
    let fuel = request.fuel.unwrap_or(state.default_fuel);
    if fuel == 0 || fuel > MAX_FUEL {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("fuel must be between 1 and {MAX_FUEL}"),
        ));
    }

    let payload = serde_json::to_vec(&request.payload_json).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("payload serialization failed: {error}"),
        )
    })?;
    if payload.len() > MAX_IO_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload exceeds invocation input limit".to_owned(),
        ));
    }

    let permit = state
        .permits
        .clone()
        .acquire_owned()
        .await
        .map_err(internal_error)?;
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let key = DeploymentKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let module = ensure_module(&state, &key).await.map_err(internal_error)?;
    let engine = state.engine.clone();
    let max_memory_bytes = state.max_memory_bytes;
    let ticks = timeout_ms.div_ceil(EPOCH_TICK_MS).max(1);

    let task = tokio::task::spawn_blocking(move || {
        execute_module(&engine, &module, payload, fuel, ticks, max_memory_bytes)
    });
    let result = task.await.map_err(internal_error)?;
    drop(permit);
    state.completed.fetch_add(1, Ordering::Relaxed);

    let response = match result {
        Ok(execution) => {
            let payload_json =
                serde_json::from_slice::<Value>(&execution.output).map_err(|error| {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("guest returned invalid JSON: {error}"),
                    )
                })?;
            InvocationResponse {
                invocation_id: request.invocation_id,
                deployment_id: request.deployment_id,
                ok: true,
                payload_json: Some(payload_json),
                error: None,
                fuel_consumed: Some(execution.fuel_consumed),
            }
        }
        Err(error) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: false,
            payload_json: None,
            error: Some(error.to_string()),
            fuel_consumed: None,
        },
    };
    return Ok(Json(response));
}

fn execute_module(
    engine: &Engine,
    module: &Module,
    input: Vec<u8>,
    fuel: u64,
    epoch_ticks: u64,
    max_memory_bytes: usize,
) -> Result<ExecutionResult> {
    let state = HostState {
        input,
        output: Vec::new(),
        log_bytes: 0,
        limits: StoreLimitsBuilder::new()
            .memory_size(max_memory_bytes)
            .instances(1)
            .memories(1)
            .tables(4)
            .table_elements(100_000)
            .trap_on_grow_failure(true)
            .build(),
    };
    let mut store = Store::new(engine, state);
    store.limiter(|state| &mut state.limits);
    store.set_fuel(fuel)?;
    store.set_epoch_deadline(epoch_ticks);
    store.epoch_deadline_trap();

    let mut linker = Linker::new(engine);
    add_hostcalls(&mut linker)?;
    let instance = linker.instantiate(&mut store, module)?;
    let entry = instance
        .get_typed_func::<(), i32>(&mut store, "wasmx_main")
        .map_err(|error| anyhow!("guest must export wasmx_main() -> i32: {error}"))?;
    let code = entry.call(&mut store, ())?;
    if code != 0 {
        bail!("guest returned non-zero status {code}");
    }

    let remaining = store.get_fuel()?;
    let state = store.into_data();
    if state.output.len() > MAX_IO_BYTES {
        bail!("guest output exceeded {MAX_IO_BYTES} bytes");
    }
    return Ok(ExecutionResult {
        output: state.output,
        fuel_consumed: fuel.saturating_sub(remaining),
    });
}

fn add_hostcalls(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "wasmx",
        "input_len",
        |caller: Caller<'_, HostState>| -> i32 {
            return i32::try_from(caller.data().input.len()).unwrap_or(i32::MAX);
        },
    )?;

    linker.func_wrap(
        "wasmx",
        "input_read",
        |mut caller: Caller<'_, HostState>, offset: i32, ptr: i32, len: i32| -> i32 {
            return match input_read(&mut caller, offset, ptr, len) {
                Ok(written) => written,
                Err(error) => {
                    tracing::warn!(%error, "guest input_read rejected");
                    -1
                }
            };
        },
    )?;

    linker.func_wrap(
        "wasmx",
        "output_write",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            return match read_guest_bytes(&mut caller, ptr, len) {
                Ok(bytes) => {
                    let new_len = caller.data().output.len().saturating_add(bytes.len());
                    if new_len > MAX_IO_BYTES {
                        return -2;
                    }
                    caller.data_mut().output.extend_from_slice(&bytes);
                    i32::try_from(bytes.len()).unwrap_or(-1)
                }
                Err(error) => {
                    tracing::warn!(%error, "guest output_write rejected");
                    -1
                }
            };
        },
    )?;

    linker.func_wrap(
        "wasmx",
        "log",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            return match read_guest_bytes(&mut caller, ptr, len) {
                Ok(bytes) => {
                    let new_len = caller.data().log_bytes.saturating_add(bytes.len());
                    if new_len > MAX_LOG_BYTES {
                        return -2;
                    }
                    caller.data_mut().log_bytes = new_len;
                    let message = String::from_utf8_lossy(&bytes);
                    tracing::info!(guest = %message, "wasmx guest log");
                    0
                }
                Err(error) => {
                    tracing::warn!(%error, "guest log rejected");
                    -1
                }
            };
        },
    )?;
    return Ok(());
}

fn input_read(caller: &mut Caller<'_, HostState>, offset: i32, ptr: i32, len: i32) -> Result<i32> {
    let offset = non_negative_usize("offset", offset)?;
    let ptr = non_negative_usize("ptr", ptr)?;
    let len = non_negative_usize("len", len)?;
    if len > MAX_HOSTCALL_BYTES {
        bail!("input_read length exceeds per-hostcall limit");
    }
    let input = caller.data().input.clone();
    let start = offset.min(input.len());
    let end = start.saturating_add(len).min(input.len());
    let slice = &input[start..end];
    let memory = guest_memory(caller)?;
    memory.write(caller, ptr, slice)?;
    return Ok(i32::try_from(slice.len()).unwrap_or(i32::MAX));
}

fn read_guest_bytes(caller: &mut Caller<'_, HostState>, ptr: i32, len: i32) -> Result<Vec<u8>> {
    let ptr = non_negative_usize("ptr", ptr)?;
    let len = non_negative_usize("len", len)?;
    if len > MAX_HOSTCALL_BYTES {
        bail!("guest buffer length exceeds per-hostcall limit");
    }
    let memory = guest_memory(caller)?;
    let mut bytes = vec![0_u8; len];
    memory.read(caller, ptr, &mut bytes)?;
    return Ok(bytes);
}

fn guest_memory(caller: &mut Caller<'_, HostState>) -> Result<wasmtime::Memory> {
    return caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| anyhow!("guest must export memory"));
}

fn non_negative_usize(name: &str, value: i32) -> Result<usize> {
    return usize::try_from(value).with_context(|| format!("{name} must be non-negative"));
}

fn validate_module_contract(module: &Module) -> Result<()> {
    for import in module.imports() {
        if import.module() != "wasmx" {
            bail!(
                "guest import {}.{} is forbidden; wasmx-v1 exposes only the wasmx module",
                import.module(),
                import.name()
            );
        }
        let ExternType::Func(actual) = import.ty() else {
            bail!("guest import wasmx.{} must be a function", import.name());
        };
        let (params, results): (&[ValType], &[ValType]) = match import.name() {
            "input_len" => (&[], &[ValType::I32]),
            "input_read" => (&[ValType::I32, ValType::I32, ValType::I32], &[ValType::I32]),
            "output_write" | "log" => (&[ValType::I32, ValType::I32], &[ValType::I32]),
            other => bail!("guest import wasmx.{other} is not part of wasmx-v1"),
        };
        let expected = FuncType::new(
            module.engine(),
            params.iter().cloned(),
            results.iter().cloned(),
        );
        if !FuncType::eq(&actual, &expected) {
            bail!(
                "guest import wasmx.{} has the wrong signature",
                import.name()
            );
        }
    }

    let mut has_memory = false;
    let mut has_entry = false;
    for export in module.exports() {
        match export.name() {
            "memory" => {
                if !matches!(export.ty(), ExternType::Memory(_)) {
                    bail!("guest export memory must be WebAssembly memory");
                }
                has_memory = true;
            }
            "wasmx_main" => {
                let ExternType::Func(actual) = export.ty() else {
                    bail!("guest export wasmx_main must be a function");
                };
                let expected = FuncType::new(module.engine(), [], [ValType::I32]);
                if !FuncType::eq(&actual, &expected) {
                    bail!("guest export wasmx_main must have signature () -> i32");
                }
                has_entry = true;
            }
            _ => {}
        }
    }
    if !has_memory {
        bail!("module must export memory");
    }
    if !has_entry {
        bail!("module must export wasmx_main");
    }
    return Ok(());
}

async fn ensure_module(state: &AppState, key: &DeploymentKey) -> Result<Module> {
    if let Some(module) = state.modules.read().await.get(key).cloned() {
        return Ok(module);
    }
    let path = artifact_path(&state.artifact_root, key)?;
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("deployment artifact not found: {}", path.display()))?;
    let module = compile_module(state, bytes.clone()).await?;
    verify_or_repair_manifest(state, key, &bytes).await?;
    cache_module(state, key.clone(), module.clone()).await;
    return Ok(module);
}

async fn compile_module(state: &AppState, bytes: Vec<u8>) -> Result<Module> {
    let _permit = state.compile_permits.clone().acquire_owned().await?;
    let engine = state.engine.clone();
    return tokio::task::spawn_blocking(move || {
        let module = Module::new(&engine, &bytes)?;
        validate_module_contract(&module)?;
        return Ok::<Module, anyhow::Error>(module);
    })
    .await
    .map_err(|error| anyhow!("Wasm compile task failed: {error}"))?;
}

async fn cache_module(state: &AppState, key: DeploymentKey, module: Module) {
    let mut modules = state.modules.write().await;
    if !modules.contains_key(&key)
        && modules.len() >= state.max_cached_modules
        && let Some(victim) = modules.keys().next().cloned()
    {
        modules.remove(&victim);
    }
    modules.insert(key, module);
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("artifact path has no parent"))?;
    tokio::fs::create_dir_all(parent).await?;
    let temp = parent.join(format!(".{}.tmp", Uuid::new_v4().simple()));
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, path).await?;
    return Ok(());
}

fn artifact_dir(root: &Path, key: &DeploymentKey) -> Result<PathBuf> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    Ok(root.join(&key.tenant_id).join(&key.deployment_id))
}

fn artifact_path(root: &Path, key: &DeploymentKey) -> Result<PathBuf> {
    Ok(artifact_dir(root, key)?.join("module.wasm"))
}

fn manifest_path(root: &Path, key: &DeploymentKey) -> Result<PathBuf> {
    Ok(artifact_dir(root, key)?.join("manifest.json"))
}

async fn write_manifest(
    root: &Path,
    key: &DeploymentKey,
    manifest: &DeploymentManifest,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    atomic_write(&manifest_path(root, key)?, &bytes).await
}

async fn read_manifest(root: &Path, key: &DeploymentKey) -> Result<DeploymentManifest> {
    let path = manifest_path(root, key)?;
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("deployment manifest not found: {}", path.display()))?;
    let manifest: DeploymentManifest = serde_json::from_slice(&bytes)
        .with_context(|| format!("deployment manifest is invalid: {}", path.display()))?;
    validate_manifest(key, &manifest)?;
    Ok(manifest)
}

fn validate_manifest(key: &DeploymentKey, manifest: &DeploymentManifest) -> Result<()> {
    if manifest.schema_version != "wasmx.deployment/v1"
        || manifest.tenant_id != key.tenant_id
        || manifest.deployment_id != key.deployment_id
        || manifest.guest_abi != WASMX_GUEST_ABI
        || manifest.target_triple != WASMX_TARGET_TRIPLE
        || manifest.wasi_enabled
    {
        bail!("deployment manifest does not match the wasm-xprs runtime contract");
    }
    if manifest.sha256.len() != 64
        || !manifest
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("deployment manifest sha256 is invalid");
    }
    Ok(())
}

async fn verify_or_repair_manifest(
    state: &AppState,
    key: &DeploymentKey,
    bytes: &[u8],
) -> Result<DeploymentManifest> {
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    match read_manifest(&state.artifact_root, key).await {
        Ok(manifest) => {
            if manifest.sha256 != sha256 || manifest.module_bytes != bytes.len() as u64 {
                bail!("deployment artifact integrity check failed");
            }
            Ok(manifest)
        }
        Err(error) if error.to_string().contains("manifest not found") => {
            let manifest = DeploymentManifest {
                schema_version: "wasmx.deployment/v1".to_owned(),
                tenant_id: key.tenant_id.clone(),
                deployment_id: key.deployment_id.clone(),
                sha256,
                module_bytes: bytes.len() as u64,
                guest_abi: WASMX_GUEST_ABI.to_owned(),
                target_triple: WASMX_TARGET_TRIPLE.to_owned(),
                wasi_enabled: false,
                ores_adapter_verified: false,
            };
            write_manifest(&state.artifact_root, key, &manifest).await?;
            Ok(manifest)
        }
        Err(error) => Err(error),
    }
}

async fn deployment_summary(
    state: &AppState,
    key: &DeploymentKey,
    cached: bool,
) -> Result<DeploymentSummary> {
    let path = artifact_path(&state.artifact_root, key)?;
    let metadata = tokio::fs::metadata(&path)
        .await
        .with_context(|| format!("deployment artifact not found: {}", path.display()))?;
    if !metadata.is_file() {
        bail!("deployment artifact is not a regular file");
    }

    let manifest = match read_manifest(&state.artifact_root, key).await {
        Ok(manifest) if manifest.module_bytes == metadata.len() => manifest,
        Ok(_) => bail!("deployment manifest size does not match module"),
        Err(error) if error.to_string().contains("manifest not found") => {
            let bytes = tokio::fs::read(&path).await?;
            verify_or_repair_manifest(state, key, &bytes).await?
        }
        Err(error) => return Err(error),
    };

    Ok(DeploymentSummary {
        tenant_id: key.tenant_id.clone(),
        deployment_id: key.deployment_id.clone(),
        sha256: manifest.sha256,
        module_bytes: manifest.module_bytes,
        cached,
        integrity_verified: cached,
        ores_adapter_verified: manifest.ores_adapter_verified,
    })
}

async fn enforce_tenant_quota(state: &AppState, tenant_id: &str, incoming_bytes: u64) -> Result<()> {
    validate_path_component(tenant_id)?;
    let tenant_dir = state.artifact_root.join(tenant_id);
    let mut deployments = 0usize;
    let mut storage_bytes = 0u64;

    let mut entries = match tokio::fs::read_dir(&tenant_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if incoming_bytes > state.max_tenant_storage_bytes {
                bail!("tenant storage quota exceeded");
            }
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        deployments = deployments.saturating_add(1);
        let module = entry.path().join("module.wasm");
        if let Ok(metadata) = tokio::fs::metadata(module).await
            && metadata.is_file()
        {
            storage_bytes = storage_bytes.saturating_add(metadata.len());
        }
    }

    if deployments >= state.max_tenant_deployments {
        bail!("tenant deployment-count quota exceeded");
    }
    if storage_bytes.saturating_add(incoming_bytes) > state.max_tenant_storage_bytes {
        bail!("tenant storage quota exceeded");
    }
    Ok(())
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if let Some(provided) = provided {
        let expected = state.token.as_bytes();
        let candidate = provided.as_bytes();
        if candidate.len() == expected.len() && bool::from(candidate.ct_eq(expected)) {
            return Ok(());
        }
    }
    return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()));
}

fn validate_identifier(name: &str, value: &str) -> Result<(), (StatusCode, String)> {
    return validate_path_component(value)
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid {name}")));
}

fn validate_path_component(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != "..";
    if !valid {
        bail!("invalid path component");
    }
    return Ok(());
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value
        .parse()
        .context("WASMX_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("WASMX_DESKTOP_ADDR must bind to loopback");
    }
    return Ok(addr);
}

fn is_loopback(ip: IpAddr) -> bool {
    return ip.is_loopback();
}

fn positive_usize_env(name: &str, default_value: usize) -> Result<usize> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(default_value);
    };
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("{name} must be an integer"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    return Ok(value);
}

fn positive_u64_env(name: &str, default_value: u64) -> Result<u64> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(default_value);
    };
    let value = raw
        .parse::<u64>()
        .with_context(|| format!("{name} must be an integer"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    return Ok(value);
}

fn artifact_root() -> Result<PathBuf> {
    if let Ok(path) = env::var("WASMX_ARTIFACT_ROOT") {
        return expand_home(Path::new(&path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    return Ok(PathBuf::from(home).join(".wasm-xprs/artifacts"));
}

fn token_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("WASMX_DESKTOP_TOKEN_FILE") {
        return expand_home(Path::new(&path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    return Ok(PathBuf::from(home).join(".wasm-xprs/daemon/token"));
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    return Ok(path.to_path_buf());
}

fn load_or_create_token(path: &Path) -> Result<String> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("desktop daemon token path must not be a symlink");
            }
            if !metadata.is_file() {
                bail!("desktop daemon token path must be a regular file");
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
            let token = std::fs::read_to_string(path)?;
            let token = token.trim();
            if token.len() >= 32 {
                return Ok(token.to_owned());
            }
            bail!("desktop daemon token file is too short");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("token path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(path)?;
    {
        use std::io::Write as _;
        file.write_all(format!("{token}\n").as_bytes())?;
        file.sync_all()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    return Ok(token);
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::error!(error = %error, "internal daemon error");
    return (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".to_owned(),
    );
}

async fn epoch_ticker(engine: Engine) {
    let mut interval = tokio::time::interval(Duration::from_millis(EPOCH_TICK_MS));
    loop {
        interval.tick().await;
        engine.increment_epoch();
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_engine() -> Result<Engine> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        return Ok(Engine::new(&config)?);
    }

    #[test]
    fn echo_guest_round_trips_json() -> Result<()> {
        let engine = test_engine()?;
        let wasm = wat::parse_str(
            r#"(module
                (import "wasmx" "input_len"
                    (func $input_len (result i32)))
                (import "wasmx" "input_read"
                    (func $input_read
                        (param i32 i32 i32) (result i32)))
                (import "wasmx" "output_write"
                    (func $output_write
                        (param i32 i32) (result i32)))
                (memory (export "memory") 1)
                (func (export "wasmx_main") (result i32)
                    (local $n i32)
                    call $input_len
                    local.set $n
                    i32.const 0
                    i32.const 0
                    local.get $n
                    call $input_read
                    drop
                    i32.const 0
                    local.get $n
                    call $output_write
                    drop
                    i32.const 0))"#,
        )?;
        let module = Module::new(&engine, wasm)?;
        validate_module_contract(&module)?;
        let input = br#"{"hello":"world"}"#.to_vec();
        let result = execute_module(&engine, &module, input.clone(), 1_000_000, 100, 1024 * 1024)?;
        assert_eq!(result.output, input);
        assert!(result.fuel_consumed > 0);
        return Ok(());
    }

    #[test]
    fn module_contract_requires_memory_and_entry() -> Result<()> {
        let engine = test_engine()?;
        let module = Module::new(&engine, wat::parse_str("(module)")?)?;
        assert!(validate_module_contract(&module).is_err());
        return Ok(());
    }

    #[test]
    fn module_contract_rejects_wasi_and_unknown_hostcalls() -> Result<()> {
        let engine = test_engine()?;
        let wasi = Module::new(
            &engine,
            wat::parse_str(
                r#"(module
                    (import "wasi_snapshot_preview1" "fd_write"
                        (func (param i32 i32 i32 i32) (result i32)))
                    (memory (export "memory") 1)
                    (func (export "wasmx_main") (result i32) i32.const 0))"#,
            )?,
        )?;
        assert!(validate_module_contract(&wasi).is_err());

        let unknown = Module::new(
            &engine,
            wat::parse_str(
                r#"(module
                    (import "wasmx" "network_open" (func (result i32)))
                    (memory (export "memory") 1)
                    (func (export "wasmx_main") (result i32) i32.const 0))"#,
            )?,
        )?;
        assert!(validate_module_contract(&unknown).is_err());
        return Ok(());
    }

    #[test]
    fn module_contract_rejects_wrong_hostcall_signature() -> Result<()> {
        let engine = test_engine()?;
        let module = Module::new(
            &engine,
            wat::parse_str(
                r#"(module
                    (import "wasmx" "input_len" (func (param i32) (result i32)))
                    (memory (export "memory") 1)
                    (func (export "wasmx_main") (result i32) i32.const 0))"#,
            )?,
        )?;
        assert!(validate_module_contract(&module).is_err());
        return Ok(());
    }
}

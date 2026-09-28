mod ores_adapter;
mod ores_receipt;

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
use cap_fs_ext::{
    DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _, OpenOptionsMaybeDirExt as _,
};
use cap_std::{
    ambient_authority,
    fs::{Dir, DirBuilder as CapDirBuilder, File as CapFile, OpenOptions as CapOpenOptions},
};
use ores_adapter::{
    OresDeploymentProvenance, OresLambdaAdapterV1, WASMX_GUEST_ABI, WASMX_TARGET_TRIPLE,
};
use ores_receipt::OresBuildEvidence;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    io::Read,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock, Semaphore};
use uuid::Uuid;
use wasmparser::{Parser, Payload};
use wasmtime::{
    Caller, Config, Engine, Extern, ExternType, FuncType, Linker, Module, Store, StoreLimits,
    StoreLimitsBuilder, ValType,
};

const DEFAULT_ADDR: &str = "127.0.0.1:8766";
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
const MAX_MANIFEST_BYTES: usize = 256 * 1024;
const MAX_IO_BYTES: usize = 10 * 1024 * 1024;
const MAX_HOSTCALL_BYTES: usize = 64 * 1024;
const MAX_LOG_BYTES: usize = 64 * 1024;
const MAX_QUEUE_WAIT_MS: u64 = 30_000;
const MAX_COMPILE_QUEUE_WAIT_MS: u64 = 30_000;
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
    artifact_dir: Arc<Dir>,
    engine: Engine,
    modules: Arc<RwLock<HashMap<DeploymentKey, Module>>>,
    permits: Arc<Semaphore>,
    compile_permits: Arc<Semaphore>,
    deployment_mutations: Arc<Mutex<()>>,
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
    #[serde(default)]
    ores_adapter_raw_base64: Option<String>,
    #[serde(default)]
    ores_receipt_raw_base64: Option<String>,
}

#[derive(Debug, Serialize)]
struct DeployResponse {
    tenant_id: String,
    deployment_id: String,
    sha256: String,
    module_bytes: usize,
    compiled: bool,
    ores_adapter_verified: bool,
    ores_receipt_verified: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ores_provenance: Option<OresDeploymentProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ores_build_evidence: Option<OresBuildEvidence>,
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
    ores_provenance_bound: bool,
    ores_adapter_sha256: Option<String>,
    ores_source: Option<String>,
    ores_source_sha256: Option<String>,
    ores_build_receipt_bound: bool,
    ores_receipt_sha256: Option<String>,
    ores_adapter_raw_sha256: Option<String>,
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
    harden_directory_permissions(&artifact_root)?;
    let artifact_dir = open_artifact_root(&artifact_root)?;
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
    let engine = Engine::new(&config)?;

    let state = AppState {
        token: Arc::from(token),
        artifact_root: Arc::new(artifact_root),
        artifact_dir: Arc::new(artifact_dir),
        engine: engine.clone(),
        modules: Arc::new(RwLock::new(HashMap::new())),
        permits: Arc::new(Semaphore::new(max_parallel)),
        compile_permits: Arc::new(Semaphore::new(max_parallel_compiles)),
        deployment_mutations: Arc::new(Mutex::new(())),
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

async fn ready(State(state): State<AppState>) -> Result<&'static str, (StatusCode, &'static str)> {
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

    let (ores_adapter_verified, ores_provenance) = match request.ores_adapter.as_ref() {
        Some(adapter) => {
            let provenance = adapter.deployment_provenance().map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("ORES adapter validation failed: {error}"),
                )
            })?;
            (true, Some(provenance))
        }
        None => (false, None),
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

    let ores_build_evidence = match (
        request.ores_adapter.as_ref(),
        request.ores_adapter_raw_base64.as_deref(),
        request.ores_receipt_raw_base64.as_deref(),
    ) {
        (Some(adapter), Some(raw_adapter), Some(raw_receipt)) => {
            let raw_adapter = BASE64.decode(raw_adapter.as_bytes()).map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "ores_adapter_raw_base64 is not valid base64".to_owned(),
                )
            })?;
            let raw_receipt = BASE64.decode(raw_receipt.as_bytes()).map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "ores_receipt_raw_base64 is not valid base64".to_owned(),
                )
            })?;
            Some(
                ores_receipt::verify(adapter, &raw_adapter, &raw_receipt, &bytes).map_err(
                    |error| {
                        (
                            StatusCode::BAD_REQUEST,
                            format!("ORES WASM receipt validation failed: {error}"),
                        )
                    },
                )?,
            )
        }
        (_, None, None) => None,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "ORES receipt evidence requires ores_adapter plus exact raw adapter and receipt bytes"
                    .to_owned(),
            ));
        }
    };

    let module = compile_module(&state, bytes.clone())
        .await
        .map_err(|error| {
            let message = error.to_string();
            if message.contains("compile queue timeout") {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "runtime compile capacity is busy".to_owned(),
                )
            } else {
                (
                    StatusCode::BAD_REQUEST,
                    format!("module validation failed: {message}"),
                )
            }
        })?;

    let key = DeploymentKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let sha256 = format!("{:x}", Sha256::digest(&bytes));

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
        ores_provenance,
        ores_build_evidence: ores_build_evidence.clone(),
    };

    // Serialize deployment mutations so quota accounting and immutable-ID checks
    // cannot race with concurrent deploy/delete requests. Every component below
    // the retained artifact-root capability is opened without following links.
    let _mutation_guard = state.deployment_mutations.lock().await;
    let tenant =
        open_tenant_directory(&state.artifact_dir, &key.tenant_id, true).map_err(internal_error)?;

    match tenant.open_dir_nofollow(&key.deployment_id) {
        Ok(deployment) => {
            let existing =
                read_regular_file_no_symlink_at(&deployment, "module.wasm", MAX_MODULE_BYTES)
                    .await
                    .map_err(internal_error)?;
            if existing != bytes {
                return Err((
                    StatusCode::CONFLICT,
                    "deployment_id is immutable and already contains a different module".to_owned(),
                ));
            }

            let existing_manifest = verify_manifest_integrity(&deployment, &key, &existing)
                .await
                .map_err(|_| {
                    (
                        StatusCode::CONFLICT,
                        "existing deployment failed immutable manifest verification".to_owned(),
                    )
                })?;
            if existing_manifest != manifest {
                return Err((
                    StatusCode::CONFLICT,
                    "deployment_id is immutable and already contains different manifest evidence"
                        .to_owned(),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            enforce_tenant_quota(&state, &tenant, bytes.len() as u64)
                .await
                .map_err(|error| (StatusCode::PAYLOAD_TOO_LARGE, error.to_string()))?;
            let deployment =
                create_deployment_directory(&tenant, &key.deployment_id).map_err(internal_error)?;
            if let Err(error) = atomic_write_at(&deployment, "module.wasm", &bytes).await {
                let _ = deployment.remove_open_dir_all();
                return Err(internal_error(error));
            }
            if let Err(error) = write_manifest(&deployment, &key, &manifest).await {
                // Do not leave a newly-created deployment half-committed.
                let _ = deployment.remove_open_dir_all();
                return Err(internal_error(error));
            }
        }
        Err(error) => return Err(internal_error(error)),
    }
    drop(_mutation_guard);
    cache_module(&state, key, module).await;

    return Ok(Json(DeployResponse {
        tenant_id: request.tenant_id,
        deployment_id: request.deployment_id,
        sha256,
        module_bytes: bytes.len(),
        compiled: true,
        ores_adapter_verified,
        ores_receipt_verified: ores_build_evidence.is_some(),
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
    for key in list_deployment_keys(&state.artifact_dir)
        .await
        .map_err(internal_error)?
    {
        let summary = deployment_summary(&state, &key, cached.contains(&key))
            .await
            .map_err(internal_error)?;
        deployments.push(summary);
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
    let _mutation_guard = state.deployment_mutations.lock().await;
    let deployment = open_deployment_directory(&state.artifact_dir, &key).map_err(|error| {
        if error.to_string().contains("not found") {
            (StatusCode::NOT_FOUND, "deployment not found".to_owned())
        } else {
            internal_error(error)
        }
    })?;
    state.modules.write().await.remove(&key);
    return match deployment.remove_open_dir_all() {
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

    let queue_started = Instant::now();
    let queue_wait_ms = timeout_ms.min(MAX_QUEUE_WAIT_MS);
    let permit = tokio::time::timeout(
        Duration::from_millis(queue_wait_ms),
        state.permits.clone().acquire_owned(),
    )
    .await
    .map_err(|_| {
        (
            StatusCode::REQUEST_TIMEOUT,
            "invocation queue timeout".to_owned(),
        )
    })?
    .map_err(internal_error)?;
    let queued_ms = u64::try_from(queue_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let remaining_timeout_ms = timeout_ms.saturating_sub(queued_ms);
    if remaining_timeout_ms == 0 {
        return Err((
            StatusCode::REQUEST_TIMEOUT,
            "invocation deadline expired while queued".to_owned(),
        ));
    }
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let key = DeploymentKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let module = ensure_module(&state, &key)
        .await
        .map_err(module_load_error)?;
    let engine = state.engine.clone();
    let max_memory_bytes = state.max_memory_bytes;
    let ticks = remaining_timeout_ms.div_ceil(EPOCH_TICK_MS).max(1);

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
    let deployment = open_deployment_directory(&state.artifact_dir, key)?;
    let bytes = read_regular_file_no_symlink_at(&deployment, "module.wasm", MAX_MODULE_BYTES)
        .await
        .context("deployment artifact not found")?;

    // Compiled code is only an optimization. Durable deployment authority stays
    // on disk and is revalidated before every cache reuse or cache refill.
    verify_manifest_integrity(&deployment, key, &bytes).await?;
    if let Some(module) = state.modules.read().await.get(key).cloned() {
        return Ok(module);
    }

    let module = compile_module(state, bytes).await?;
    cache_module(state, key.clone(), module.clone()).await;
    return Ok(module);
}

async fn compile_module(state: &AppState, bytes: Vec<u8>) -> Result<Module> {
    let _permit = tokio::time::timeout(
        Duration::from_millis(MAX_COMPILE_QUEUE_WAIT_MS),
        state.compile_permits.clone().acquire_owned(),
    )
    .await
    .context("Wasm compile queue timeout")??;
    let engine = state.engine.clone();
    return tokio::task::spawn_blocking(move || {
        validate_wasm_feature_surface(&bytes)?;
        let module = Module::new(&engine, &bytes)?;
        validate_module_contract(&module)?;
        return Ok::<Module, anyhow::Error>(module);
    })
    .await
    .map_err(|error| anyhow!("Wasm compile task failed: {error}"))?;
}

fn validate_memory_type(memory: wasmparser::MemoryType) -> Result<()> {
    if memory.shared {
        bail!("shared WebAssembly memory/threads are not allowed by wasmx-v1");
    }
    if memory.memory64 {
        bail!("memory64 is not allowed by wasmx-v1");
    }
    Ok(())
}

fn validate_wasm_feature_surface(bytes: &[u8]) -> Result<()> {
    let mut memory_count = 0usize;
    for payload in Parser::new(0).parse_all(bytes) {
        if let Payload::MemorySection(section) = payload? {
            for memory in section {
                memory_count = memory_count.saturating_add(1);
                if memory_count > 1 {
                    bail!("wasmx-v1 allows exactly one guest linear memory");
                }
                validate_memory_type(memory?)?;
            }
        }
    }
    Ok(())
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

fn open_artifact_root(root: &Path) -> Result<Dir> {
    let mut options = CapOpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.maybe_dir(true);
    let file =
        CapFile::open_ambient_with(root, &options, ambient_authority()).with_context(|| {
            format!(
                "could not open artifact root without following links: {}",
                root.display()
            )
        })?;
    if !file.metadata()?.is_dir() {
        bail!("artifact root is not a directory: {}", root.display());
    }
    Ok(Dir::from_std_file(file.into_std()))
}

fn create_cap_directory(parent: &Dir, name: &str) -> std::io::Result<()> {
    let mut builder = CapDirBuilder::new();
    #[cfg(unix)]
    {
        use cap_std::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    parent.create_dir_with(name, &builder)
}

fn open_tenant_directory(root: &Dir, tenant_id: &str, create_missing: bool) -> Result<Dir> {
    validate_path_component(tenant_id)?;
    match root.open_dir_nofollow(tenant_id) {
        Ok(directory) => Ok(directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && create_missing => {
            match create_cap_directory(root, tenant_id) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            root.open_dir_nofollow(tenant_id).with_context(|| {
                format!("tenant artifact directory is not a real directory: {tenant_id}")
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!("deployment not found: tenant {tenant_id}")
        }
        Err(error) => Err(error.into()),
    }
}

fn create_deployment_directory(tenant: &Dir, deployment_id: &str) -> Result<Dir> {
    validate_path_component(deployment_id)?;
    create_cap_directory(tenant, deployment_id)
        .with_context(|| format!("could not create deployment directory: {deployment_id}"))?;
    tenant.open_dir_nofollow(deployment_id).with_context(|| {
        format!("deployment directory is not a real directory: {deployment_id}")
    })
}

fn open_deployment_directory(root: &Dir, key: &DeploymentKey) -> Result<Dir> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    let tenant = open_tenant_directory(root, &key.tenant_id, false)?;
    tenant
        .open_dir_nofollow(&key.deployment_id)
        .with_context(|| {
            format!(
                "deployment not found: {}/{}",
                key.tenant_id, key.deployment_id
            )
        })
}

async fn atomic_write_at(directory: &Dir, name: &str, bytes: &[u8]) -> Result<()> {
    validate_path_component(name)?;
    let directory = directory.try_clone()?;
    let name = name.to_owned();
    let bytes = bytes.to_vec();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::Write as _;

        let temp = format!(".{}.tmp", Uuid::new_v4().simple());
        let mut options = CapOpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        #[cfg(unix)]
        {
            use cap_std::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }

        let mut file = directory.open_with(&temp, &options)?;
        if let Err(error) = file.write_all(&bytes).and_then(|_| file.sync_all()) {
            let _ = directory.remove_file(&temp);
            return Err(error.into());
        }
        drop(file);

        // Capability-relative hard-link publication is atomic no-replace within
        // this already-open deployment directory.
        if let Err(error) = directory.hard_link(&temp, &directory, &name) {
            let _ = directory.remove_file(&temp);
            return Err(error.into());
        }
        if let Err(error) = directory.remove_file(&temp) {
            tracing::warn!(temporary = %temp, %error, "published artifact but could not remove staging hard-link");
        }

        #[cfg(unix)]
        directory.try_clone()?.into_std_file().sync_all()?;
        Ok(())
    })
    .await
    .map_err(|error| anyhow!("capability-relative artifact write task failed: {error}"))?
}

async fn read_regular_file_no_symlink_at(
    directory: &Dir,
    name: &str,
    limit: usize,
) -> Result<Vec<u8>> {
    validate_path_component(name)?;
    let directory = directory.try_clone()?;
    let name = name.to_owned();
    tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let mut options = CapOpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let file = directory.open_with(&name, &options)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("artifact is not a regular non-symlink file: {name}");
        }
        if metadata.len() > limit as u64 {
            bail!("file exceeds bounded read limit: {name}");
        }

        let mut bytes = Vec::with_capacity(metadata.len().min(limit as u64) as usize);
        file.take(limit.saturating_add(1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            bail!("file exceeded bounded read limit while reading: {name}");
        }
        Ok(bytes)
    })
    .await
    .map_err(|error| anyhow!("capability-relative artifact read task failed: {error}"))?
}

async fn list_deployment_keys(root: &Dir) -> Result<Vec<DeploymentKey>> {
    let root = root.try_clone()?;
    tokio::task::spawn_blocking(move || -> Result<Vec<DeploymentKey>> {
        let mut keys = Vec::new();
        for tenant_entry in root.entries()? {
            let tenant_entry = tenant_entry?;
            let tenant_id = tenant_entry.file_name().to_string_lossy().into_owned();
            if validate_path_component(&tenant_id).is_err() {
                continue;
            }
            let tenant = match root.open_dir_nofollow(&tenant_id) {
                Ok(directory) => directory,
                Err(_) => continue,
            };
            for deployment_entry in tenant.entries()? {
                let deployment_entry = deployment_entry?;
                let deployment_id = deployment_entry.file_name().to_string_lossy().into_owned();
                if validate_path_component(&deployment_id).is_err() {
                    continue;
                }
                if tenant.open_dir_nofollow(&deployment_id).is_err() {
                    continue;
                }
                keys.push(DeploymentKey {
                    tenant_id: tenant_id.clone(),
                    deployment_id,
                });
            }
        }
        Ok(keys)
    })
    .await
    .map_err(|error| anyhow!("capability-relative deployment listing task failed: {error}"))?
}

async fn write_manifest(
    deployment: &Dir,
    key: &DeploymentKey,
    manifest: &DeploymentManifest,
) -> Result<()> {
    validate_manifest(key, manifest)?;
    let bytes = serde_json::to_vec_pretty(manifest)?;
    atomic_write_at(deployment, "manifest.json", &bytes).await
}

async fn read_manifest(deployment: &Dir, key: &DeploymentKey) -> Result<DeploymentManifest> {
    let bytes = read_regular_file_no_symlink_at(deployment, "manifest.json", MAX_MANIFEST_BYTES)
        .await
        .context("deployment manifest not found")?;
    let manifest: DeploymentManifest =
        serde_json::from_slice(&bytes).context("deployment manifest is invalid")?;
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
    match (&manifest.ores_provenance, manifest.ores_adapter_verified) {
        (Some(provenance), true) => provenance.validate()?,
        (Some(_), false) => {
            bail!("deployment manifest cannot carry ORES provenance without adapter verification");
        }
        (None, _) => {}
    }
    if let Some(evidence) = manifest.ores_build_evidence.as_ref() {
        evidence.validate()?;
        if !manifest.ores_adapter_verified || manifest.ores_provenance.is_none() {
            bail!("ORES build evidence requires verified adapter provenance");
        }
        if evidence.artifact_sha256 != manifest.sha256 {
            bail!("ORES build evidence artifact digest does not match deployment manifest");
        }
    }
    Ok(())
}

async fn verify_manifest_integrity(
    deployment: &Dir,
    key: &DeploymentKey,
    bytes: &[u8],
) -> Result<DeploymentManifest> {
    let manifest = read_manifest(deployment, key).await?;
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    if manifest.sha256 != sha256 || manifest.module_bytes != bytes.len() as u64 {
        bail!("deployment artifact integrity check failed");
    }
    Ok(manifest)
}

async fn deployment_summary(
    state: &AppState,
    key: &DeploymentKey,
    cached: bool,
) -> Result<DeploymentSummary> {
    let deployment = open_deployment_directory(&state.artifact_dir, key)?;
    let metadata = deployment
        .symlink_metadata("module.wasm")
        .context("deployment artifact not found")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("deployment artifact is not a regular non-symlink file");
    }

    let bytes =
        read_regular_file_no_symlink_at(&deployment, "module.wasm", MAX_MODULE_BYTES).await?;
    let manifest = verify_manifest_integrity(&deployment, key, &bytes).await?;
    if manifest.module_bytes != metadata.len() {
        bail!("deployment manifest size does not match module");
    }

    let (ores_provenance_bound, ores_adapter_sha256, ores_source, ores_source_sha256) =
        match manifest.ores_provenance.as_ref() {
            Some(provenance) => (
                true,
                Some(provenance.adapter_sha256.clone()),
                Some(provenance.source.clone()),
                Some(provenance.source_sha256.clone()),
            ),
            None => (false, None, None, None),
        };

    let (ores_build_receipt_bound, ores_receipt_sha256, ores_adapter_raw_sha256) =
        match manifest.ores_build_evidence.as_ref() {
            Some(evidence) => (
                true,
                Some(evidence.receipt_sha256.clone()),
                Some(evidence.adapter_raw_sha256.clone()),
            ),
            None => (false, None, None),
        };

    Ok(DeploymentSummary {
        tenant_id: key.tenant_id.clone(),
        deployment_id: key.deployment_id.clone(),
        sha256: manifest.sha256,
        module_bytes: manifest.module_bytes,
        cached,
        integrity_verified: true,
        ores_adapter_verified: manifest.ores_adapter_verified,
        ores_provenance_bound,
        ores_adapter_sha256,
        ores_source,
        ores_source_sha256,
        ores_build_receipt_bound,
        ores_receipt_sha256,
        ores_adapter_raw_sha256,
    })
}

async fn enforce_tenant_quota(state: &AppState, tenant: &Dir, incoming_bytes: u64) -> Result<()> {
    let tenant = tenant.try_clone()?;
    let max_deployments = state.max_tenant_deployments;
    let max_storage_bytes = state.max_tenant_storage_bytes;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut deployments = 0usize;
        let mut storage_bytes = 0u64;
        for entry in tenant.entries()? {
            let entry = entry?;
            let deployment_id = entry.file_name().to_string_lossy().into_owned();
            if validate_path_component(&deployment_id).is_err() {
                continue;
            }
            let deployment = match tenant.open_dir_nofollow(&deployment_id) {
                Ok(directory) => directory,
                Err(_) => continue,
            };
            if let Ok(metadata) = deployment.symlink_metadata("module.wasm")
                && metadata.is_file()
                && !metadata.file_type().is_symlink()
            {
                deployments = deployments.saturating_add(1);
                storage_bytes = storage_bytes.saturating_add(metadata.len());
            }
        }

        if deployments >= max_deployments {
            bail!("tenant deployment-count quota exceeded");
        }
        if storage_bytes.saturating_add(incoming_bytes) > max_storage_bytes {
            bail!("tenant storage quota exceeded");
        }
        Ok(())
    })
    .await
    .map_err(|error| anyhow!("capability-relative tenant quota task failed: {error}"))?
}

fn harden_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn module_load_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    if message.contains("deployment artifact not found") {
        return (StatusCode::NOT_FOUND, "deployment not found".to_owned());
    }
    if message.contains("compile queue timeout") {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime compile capacity is busy".to_owned(),
        );
    }
    internal_error(error)
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
                let mode = metadata.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    bail!(
                        "desktop daemon token file permissions must be owner-only (0600 or stricter); found {mode:04o}"
                    );
                }
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
    harden_directory_permissions(parent)?;
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
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
    fn v1_rejects_shared_memory_before_compilation() -> Result<()> {
        let wasm = wat::parse_str(
            r#"(module
                (memory 1 1 shared)
                (export "memory" (memory 0))
                (func (export "wasmx_main") (result i32) i32.const 0))"#,
        )?;
        assert!(validate_wasm_feature_surface(&wasm).is_err());
        return Ok(());
    }

    #[test]
    fn v1_rejects_multiple_linear_memories_before_compilation() -> Result<()> {
        let wasm = wat::parse_str(
            r#"(module
                (memory 1)
                (memory 1)
                (export "memory" (memory 0))
                (func (export "wasmx_main") (result i32) i32.const 0))"#,
        )?;
        assert!(validate_wasm_feature_surface(&wasm).is_err());
        return Ok(());
    }

    #[tokio::test]
    async fn atomic_write_never_replaces_existing_artifact() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("wasmx-no-replace-test-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root)?;
        let directory = open_artifact_root(&root)?;

        atomic_write_at(&directory, "module.wasm", b"first").await?;
        assert!(
            atomic_write_at(&directory, "module.wasm", b"second")
                .await
                .is_err()
        );
        assert_eq!(
            read_regular_file_no_symlink_at(&directory, "module.wasm", 16).await?,
            b"first".to_vec()
        );

        std::fs::remove_dir_all(root)?;
        return Ok(());
    }

    #[tokio::test]
    async fn new_deployment_chain_preserves_missing_leaf_until_publish() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "wasmx-first-deploy-test-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root)?;
        let root_dir = open_artifact_root(&root)?;
        let key = DeploymentKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "deployment-a".to_owned(),
        };

        let tenant = open_tenant_directory(&root_dir, &key.tenant_id, true)?;
        assert!(tenant.open_dir_nofollow(&key.deployment_id).is_err());

        let deployment = create_deployment_directory(&tenant, &key.deployment_id)?;
        atomic_write_at(&deployment, "module.wasm", b"module").await?;
        assert_eq!(
            read_regular_file_no_symlink_at(&deployment, "module.wasm", 16).await?,
            b"module".to_vec()
        );

        std::fs::remove_dir_all(root)?;
        return Ok(());
    }

    #[cfg(unix)]
    #[test]
    fn existing_token_with_group_or_world_access_is_rejected() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root =
            std::env::temp_dir().join(format!("wasmx-token-mode-test-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root)?;
        let path = root.join("token");
        std::fs::write(&path, format!("{}\n", "a".repeat(64)))?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;

        let error = match load_or_create_token(&path) {
            Ok(_) => bail!("weak token mode must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("owner-only"));
        assert_eq!(
            std::fs::metadata(&path)?.permissions().mode() & 0o777,
            0o644
        );

        std::fs::remove_dir_all(root)?;
        return Ok(());
    }

    #[cfg(unix)]
    #[test]
    fn artifact_chain_rejects_symlinked_tenant_directory() -> Result<()> {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "wasmx-parent-symlink-test-{}",
            Uuid::new_v4().simple()
        ));
        let outside = std::env::temp_dir().join(format!(
            "wasmx-parent-symlink-outside-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(&outside)?;
        symlink(&outside, root.join("tenant-a"))?;
        let root_dir = open_artifact_root(&root)?;

        assert!(open_tenant_directory(&root_dir, "tenant-a", true).is_err());
        assert!(!outside.join("deployment-a").exists());

        std::fs::remove_dir_all(root)?;
        std::fs::remove_dir_all(outside)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn artifact_chain_rejects_symlinked_deployment_directory() -> Result<()> {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "wasmx-deployment-symlink-test-{}",
            Uuid::new_v4().simple()
        ));
        let outside = std::env::temp_dir().join(format!(
            "wasmx-deployment-symlink-outside-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(root.join("tenant-a"))?;
        std::fs::create_dir_all(&outside)?;
        symlink(&outside, root.join("tenant-a").join("deployment-a"))?;
        let root_dir = open_artifact_root(&root)?;
        let key = DeploymentKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "deployment-a".to_owned(),
        };

        assert!(open_deployment_directory(&root_dir, &key).is_err());

        std::fs::remove_dir_all(root)?;
        std::fs::remove_dir_all(outside)?;
        Ok(())
    }

    #[tokio::test]
    async fn secure_persisted_read_is_bounded() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "wasmx-secure-read-test-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root)?;
        std::fs::write(root.join("artifact.bin"), b"12345")?;
        let root_dir = open_artifact_root(&root)?;

        assert!(
            read_regular_file_no_symlink_at(&root_dir, "artifact.bin", 4)
                .await
                .is_err()
        );
        assert_eq!(
            read_regular_file_no_symlink_at(&root_dir, "artifact.bin", 5).await?,
            b"12345".to_vec()
        );

        std::fs::remove_dir_all(root)?;
        return Ok(());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn secure_persisted_read_rejects_final_component_symlink() -> Result<()> {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "wasmx-secure-symlink-test-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root)?;
        let target = root.join("target.wasm");
        let link = root.join("module.wasm");
        std::fs::write(&target, b"module")?;
        symlink(&target, &link)?;
        let root_dir = open_artifact_root(&root)?;

        assert!(
            read_regular_file_no_symlink_at(&root_dir, "module.wasm", MAX_MODULE_BYTES)
                .await
                .is_err()
        );

        std::fs::remove_dir_all(root)?;
        return Ok(());
    }

    #[tokio::test]
    async fn cached_module_revalidates_persisted_bytes_before_reuse() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "wasmx-cache-integrity-test-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root)?;
        let root_dir = open_artifact_root(&root)?;
        let key = DeploymentKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "cached-v1".to_owned(),
        };
        let tenant = open_tenant_directory(&root_dir, &key.tenant_id, true)?;
        let deployment = create_deployment_directory(&tenant, &key.deployment_id)?;

        let wasm = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "wasmx_main") (result i32) i32.const 0))"#,
        )?;
        let engine = test_engine()?;
        let module = Module::new(&engine, &wasm)?;
        validate_module_contract(&module)?;

        let state = AppState {
            token: Arc::from("test-token"),
            artifact_root: Arc::new(root.clone()),
            artifact_dir: Arc::new(root_dir),
            engine,
            modules: Arc::new(RwLock::new(HashMap::new())),
            permits: Arc::new(Semaphore::new(1)),
            compile_permits: Arc::new(Semaphore::new(1)),
            deployment_mutations: Arc::new(Mutex::new(())),
            max_cached_modules: 4,
            max_tenant_deployments: 4,
            max_tenant_storage_bytes: MAX_MODULE_BYTES as u64 * 4,
            max_memory_bytes: DEFAULT_MEMORY_BYTES,
            default_fuel: DEFAULT_FUEL,
            started_at: Instant::now(),
            accepted: Arc::new(AtomicU64::new(0)),
            completed: Arc::new(AtomicU64::new(0)),
        };

        atomic_write_at(&deployment, "module.wasm", &wasm).await?;
        let manifest = DeploymentManifest {
            schema_version: "wasmx.deployment/v1".to_owned(),
            tenant_id: key.tenant_id.clone(),
            deployment_id: key.deployment_id.clone(),
            sha256: format!("{:x}", Sha256::digest(&wasm)),
            module_bytes: wasm.len() as u64,
            guest_abi: WASMX_GUEST_ABI.to_owned(),
            target_triple: WASMX_TARGET_TRIPLE.to_owned(),
            wasi_enabled: false,
            ores_adapter_verified: false,
            ores_provenance: None,
            ores_build_evidence: None,
        };
        write_manifest(&deployment, &key, &manifest).await?;
        cache_module(&state, key.clone(), module).await;

        deployment.write("module.wasm", b"tampered")?;
        assert!(ensure_module(&state, &key).await.is_err());

        std::fs::remove_dir_all(root)?;
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
    fn deployment_manifest_is_bound_to_identity_and_runtime() -> Result<()> {
        let key = DeploymentKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "echo-v1".to_owned(),
        };
        let mut manifest = DeploymentManifest {
            schema_version: "wasmx.deployment/v1".to_owned(),
            tenant_id: key.tenant_id.clone(),
            deployment_id: key.deployment_id.clone(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            module_bytes: 42,
            guest_abi: WASMX_GUEST_ABI.to_owned(),
            target_triple: WASMX_TARGET_TRIPLE.to_owned(),
            wasi_enabled: false,
            ores_adapter_verified: true,
            ores_provenance: None,
            ores_build_evidence: None,
        };
        validate_manifest(&key, &manifest)?;
        manifest.wasi_enabled = true;
        assert!(validate_manifest(&key, &manifest).is_err());
        manifest.wasi_enabled = false;
        manifest.deployment_id = "other".to_owned();
        assert!(validate_manifest(&key, &manifest).is_err());
        return Ok(());
    }

    #[test]
    fn deployment_manifest_binds_ores_provenance_when_present() -> Result<()> {
        let key = DeploymentKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "echo-v2".to_owned(),
        };
        let legacy = DeploymentManifest {
            schema_version: "wasmx.deployment/v1".to_owned(),
            tenant_id: key.tenant_id.clone(),
            deployment_id: key.deployment_id.clone(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            module_bytes: 42,
            guest_abi: WASMX_GUEST_ABI.to_owned(),
            target_triple: WASMX_TARGET_TRIPLE.to_owned(),
            wasi_enabled: false,
            ores_adapter_verified: true,
            ores_provenance: None,
            ores_build_evidence: None,
        };
        validate_manifest(&key, &legacy)?;

        let mut bound = legacy.clone();
        bound.ores_provenance = Some(OresDeploymentProvenance {
            adapter_sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            source: "src/routes/echo/lambda.rs".to_owned(),
            source_sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .to_owned(),
        });
        validate_manifest(&key, &bound)?;
        assert_ne!(legacy, bound);

        let Some(provenance) = bound.ores_provenance.as_mut() else {
            bail!("bound provenance must exist");
        };
        provenance.adapter_sha256 = "INVALID".to_owned();
        assert!(validate_manifest(&key, &bound).is_err());

        let mut unverified = legacy;
        unverified.ores_adapter_verified = false;
        unverified.ores_provenance = Some(OresDeploymentProvenance {
            adapter_sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            source: "src/routes/echo/lambda.rs".to_owned(),
            source_sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .to_owned(),
        });
        assert!(validate_manifest(&key, &unverified).is_err());
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

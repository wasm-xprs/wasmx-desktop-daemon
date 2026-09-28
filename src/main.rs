use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{delete, get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
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
use tokio::sync::{RwLock, Semaphore};
use subtle::ConstantTimeEq;
use uuid::Uuid;
use wasmtime::{
    Caller, Config, Engine, Extern, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
};

const DEFAULT_ADDR: &str = "127.0.0.1:8765";
const DEFAULT_MEMORY_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_PARALLEL: usize = 8;
const DEFAULT_FUEL: u64 = 50_000_000;
const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_MODULE_BYTES: usize = 64 * 1024 * 1024;
const MAX_IO_BYTES: usize = 10 * 1024 * 1024;
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
    max_memory_bytes: usize,
    default_fuel: u64,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
struct DeployRequest {
    tenant_id: String,
    deployment_id: String,
    wasm_base64: String,
}

#[derive(Debug, Serialize)]
struct DeployResponse {
    tenant_id: String,
    deployment_id: String,
    sha256: String,
    module_bytes: usize,
    compiled: bool,
}

#[derive(Debug, Serialize)]
struct DeploymentSummary {
    tenant_id: String,
    deployment_id: String,
    sha256: String,
    module_bytes: u64,
    cached: bool,
}

#[derive(Debug, Deserialize)]
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
    runtime: &'static str,
    isolation: &'static str,
    guest_abi: &'static str,
    store_per_invocation: bool,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    available_slots: usize,
    cached_modules: usize,
    max_memory_bytes: usize,
    default_fuel: u64,
}

#[derive(Debug)]
struct HostState {
    input: Vec<u8>,
    output: Vec<u8>,
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
    let max_parallel =
        positive_usize_env("WASMX_MAX_PARALLEL_INVOCATIONS", DEFAULT_MAX_PARALLEL)?;
    let default_fuel = positive_u64_env("WASMX_DEFAULT_FUEL", DEFAULT_FUEL)?;

    let mut config = Config::new();
    config.consume_fuel(true);
    config.epoch_interruption(true);
    let engine = Engine::new(&config)?;

    let state = AppState {
        token: Arc::from(token),
        artifact_root: Arc::new(artifact_root),
        engine: engine.clone(),
        modules: Arc::new(RwLock::new(HashMap::new())),
        permits: Arc::new(Semaphore::new(max_parallel)),
        max_memory_bytes,
        default_fuel,
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
    };

    tokio::spawn(epoch_ticker(engine));

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/deploy", post(deploy))
        .route("/v1/deployments", get(list_deployments))
        .route(
            "/v1/deployments/{tenant_id}/{deployment_id}",
            delete(delete_deployment),
        )
        .route("/v1/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(MAX_MODULE_BYTES * 2))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "wasm-xprs desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let cached_modules = state.modules.read().await.len();
    Ok(Json(StatusResponse {
        runtime: "wasmtime",
        isolation: "fresh_store_per_invocation",
        guest_abi: "wasmx-v1",
        store_per_invocation: true,
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        available_slots: state.permits.available_permits(),
        cached_modules,
        max_memory_bytes: state.max_memory_bytes,
        default_fuel: state.default_fuel,
    }))
}

async fn deploy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DeployRequest>,
) -> Result<Json<DeployResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &request.tenant_id)?;
    validate_identifier("deployment_id", &request.deployment_id)?;

    let bytes = BASE64
        .decode(request.wasm_base64.as_bytes())
        .map_err(|_| {
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

    let module = Module::new(&state.engine, &bytes).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("module validation failed: {error}"),
        )
    })?;
    validate_module_contract(&module)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;

    let key = DeploymentKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };
    let path = artifact_path(&state.artifact_root, &key).map_err(internal_error)?;
    atomic_write(&path, &bytes).await.map_err(internal_error)?;
    state.modules.write().await.insert(key, module);

    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    Ok(Json(DeployResponse {
        tenant_id: request.tenant_id,
        deployment_id: request.deployment_id,
        sha256,
        module_bytes: bytes.len(),
        compiled: true,
    }))
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
        if !tenant_entry.file_type().await.map_err(internal_error)?.is_dir() {
            continue;
        }
        let tenant_id = tenant_entry.file_name().to_string_lossy().into_owned();
        if validate_path_component(&tenant_id).is_err() {
            continue;
        }

        let mut tenant_deployments =
            tokio::fs::read_dir(tenant_entry.path()).await.map_err(internal_error)?;
        while let Some(deployment_entry) =
            tenant_deployments.next_entry().await.map_err(internal_error)?
        {
            if !deployment_entry
                .file_type()
                .await
                .map_err(internal_error)?
                .is_dir()
            {
                continue;
            }
            let deployment_id =
                deployment_entry.file_name().to_string_lossy().into_owned();
            if validate_path_component(&deployment_id).is_err() {
                continue;
            }

            let module_path = deployment_entry.path().join("module.wasm");
            let metadata = match tokio::fs::metadata(&module_path).await {
                Ok(metadata) if metadata.is_file() => metadata,
                Ok(_) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(internal_error(error)),
            };
            let bytes = tokio::fs::read(&module_path).await.map_err(internal_error)?;
            let key = DeploymentKey {
                tenant_id: tenant_id.clone(),
                deployment_id: deployment_id.clone(),
            };
            deployments.push(DeploymentSummary {
                tenant_id: tenant_id.clone(),
                deployment_id,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                module_bytes: metadata.len(),
                cached: cached.contains(&key),
            });
        }
    }

    deployments.sort_by(|a, b| {
        (&a.tenant_id, &a.deployment_id).cmp(&(&b.tenant_id, &b.deployment_id))
    });
    Ok(Json(deployments))
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
    let path = artifact_path(&state.artifact_root, &key).map_err(internal_error)?;
    match tokio::fs::remove_file(path).await {
        Ok(()) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .map_err(internal_error),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "deployment not found".to_owned()))
        }
        Err(error) => Err(internal_error(error)),
    }
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
    if fuel == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "fuel must be greater than zero".to_owned(),
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
        execute_module(
            &engine,
            &module,
            payload,
            fuel,
            ticks,
            max_memory_bytes,
        )
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
    Ok(Json(response))
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
    Ok(ExecutionResult {
        output: state.output,
        fuel_consumed: fuel.saturating_sub(remaining),
    })
}

fn add_hostcalls(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "wasmx",
        "input_len",
        |caller: Caller<'_, HostState>| -> i32 {
            i32::try_from(caller.data().input.len()).unwrap_or(i32::MAX)
        },
    )?;

    linker.func_wrap(
        "wasmx",
        "input_read",
        |mut caller: Caller<'_, HostState>, offset: i32, ptr: i32, len: i32| -> i32 {
            match input_read(&mut caller, offset, ptr, len) {
                Ok(written) => written,
                Err(error) => {
                    tracing::warn!(%error, "guest input_read rejected");
                    -1
                }
            }
        },
    )?;

    linker.func_wrap(
        "wasmx",
        "output_write",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            match read_guest_bytes(&mut caller, ptr, len) {
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
            }
        },
    )?;

    linker.func_wrap(
        "wasmx",
        "log",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            match read_guest_bytes(&mut caller, ptr, len) {
                Ok(bytes) => {
                    let message = String::from_utf8_lossy(&bytes);
                    tracing::info!(guest = %message, "wasmx guest log");
                    0
                }
                Err(error) => {
                    tracing::warn!(%error, "guest log rejected");
                    -1
                }
            }
        },
    )?;
    Ok(())
}

fn input_read(
    caller: &mut Caller<'_, HostState>,
    offset: i32,
    ptr: i32,
    len: i32,
) -> Result<i32> {
    let offset = non_negative_usize("offset", offset)?;
    let ptr = non_negative_usize("ptr", ptr)?;
    let len = non_negative_usize("len", len)?;
    let input = caller.data().input.clone();
    let start = offset.min(input.len());
    let end = start.saturating_add(len).min(input.len());
    let slice = &input[start..end];
    let memory = guest_memory(caller)?;
    memory.write(caller, ptr, slice)?;
    Ok(i32::try_from(slice.len()).unwrap_or(i32::MAX))
}

fn read_guest_bytes(
    caller: &mut Caller<'_, HostState>,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>> {
    let ptr = non_negative_usize("ptr", ptr)?;
    let len = non_negative_usize("len", len)?;
    if len > MAX_IO_BYTES {
        bail!("guest buffer length exceeds host limit");
    }
    let memory = guest_memory(caller)?;
    let mut bytes = vec![0_u8; len];
    memory.read(caller, ptr, &mut bytes)?;
    Ok(bytes)
}

fn guest_memory(caller: &mut Caller<'_, HostState>) -> Result<wasmtime::Memory> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| anyhow!("guest must export memory"))
}

fn non_negative_usize(name: &str, value: i32) -> Result<usize> {
    usize::try_from(value).with_context(|| format!("{name} must be non-negative"))
}

fn validate_module_contract(module: &Module) -> Result<()> {
    let mut has_memory = false;
    let mut has_entry = false;
    for export in module.exports() {
        match export.name() {
            "memory" => has_memory = true,
            "wasmx_main" => has_entry = true,
            _ => {}
        }
    }
    if !has_memory {
        bail!("module must export memory");
    }
    if !has_entry {
        bail!("module must export wasmx_main");
    }
    Ok(())
}

async fn ensure_module(state: &AppState, key: &DeploymentKey) -> Result<Module> {
    if let Some(module) = state.modules.read().await.get(key).cloned() {
        return Ok(module);
    }
    let path = artifact_path(&state.artifact_root, key)?;
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("deployment artifact not found: {}", path.display()))?;
    let module = Module::new(&state.engine, &bytes)?;
    validate_module_contract(&module)?;
    let mut modules = state.modules.write().await;
    Ok(modules.entry(key.clone()).or_insert(module).clone())
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("artifact path has no parent"))?;
    tokio::fs::create_dir_all(parent).await?;
    let temp = parent.join(format!(".{}.tmp", Uuid::new_v4().simple()));
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, path).await?;
    Ok(())
}

fn artifact_path(root: &Path, key: &DeploymentKey) -> Result<PathBuf> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    Ok(root
        .join(&key.tenant_id)
        .join(&key.deployment_id)
        .join("module.wasm"))
}

fn authorize(
    headers: &HeaderMap,
    state: &AppState,
) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if let Some(provided) = provided {
        let expected = state.token.as_bytes();
        let candidate = provided.as_bytes();
        if candidate.len() == expected.len()
            && bool::from(candidate.ct_eq(expected))
        {
            return Ok(());
        }
    }
    Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()))
}

fn validate_identifier(
    name: &str,
    value: &str,
) -> Result<(), (StatusCode, String)> {
    validate_path_component(value)
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid {name}")))
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
    Ok(())
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value
        .parse()
        .context("WASMX_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("WASMX_DESKTOP_ADDR must bind to loopback");
    }
    Ok(addr)
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
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
    Ok(value)
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
    Ok(value)
}

fn artifact_root() -> Result<PathBuf> {
    if let Ok(path) = env::var("WASMX_ARTIFACT_ROOT") {
        return expand_home(Path::new(&path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    Ok(PathBuf::from(home).join(".wasm-xprs/artifacts"))
}

fn token_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("WASMX_DESKTOP_TOKEN_FILE") {
        return expand_home(Path::new(&path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    Ok(PathBuf::from(home).join(".wasm-xprs/daemon/token"))
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    Ok(path.to_path_buf())
}

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim();
        if token.len() >= 32 {
            return Ok(token.to_owned());
        }
        bail!("desktop daemon token file is too short");
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("token path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let token = format!(
        "{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    );
    std::fs::write(path, format!("{token}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            path,
            std::fs::Permissions::from_mode(0o600),
        )?;
    }
    Ok(token)
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

async fn epoch_ticker(engine: Engine) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(EPOCH_TICK_MS));
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
        Ok(Engine::new(&config)?)
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
        let input = br#"{"hello":"world"}"#.to_vec();
        let result = execute_module(
            &engine,
            &module,
            input.clone(),
            1_000_000,
            100,
            1024 * 1024,
        )?;
        assert_eq!(result.output, input);
        assert!(result.fuel_consumed > 0);
        Ok(())
    }

    #[test]
    fn module_contract_requires_memory_and_entry() -> Result<()> {
        let engine = test_engine()?;
        let module =
            Module::new(&engine, wat::parse_str("(module)")?)?;
        assert!(validate_module_contract(&module).is_err());
        Ok(())
    }
}

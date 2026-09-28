use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn temp_root(case: &str) -> Result<PathBuf, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "wasmx-security-{case}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root)?;
    return Ok(root);
}

fn unused_loopback() -> Result<SocketAddr, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    return Ok(address);
}

#[cfg(unix)]
fn occupied_loopback() -> Result<(TcpListener, SocketAddr), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    return Ok((listener, address));
}

fn secure_token(path: &Path, token: &str) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, format!("{token}\n"))?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    return Ok(());
}

fn start_daemon(
    root: &Path,
    address: SocketAddr,
    token_path: &Path,
) -> Result<Child, Box<dyn Error>> {
    let child = Command::new(env!("CARGO_BIN_EXE_wasmx-desktop-daemon"))
        .env("HOME", root)
        .env("WASMX_DESKTOP_ADDR", address.to_string())
        .env("WASMX_DESKTOP_TOKEN_FILE", token_path)
        .env("WASMX_ARTIFACT_ROOT", root.join("artifacts"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    return Ok(child);
}

fn wait_until_listening(address: SocketAddr) -> Result<(), Box<dyn Error>> {
    for _ in 0..120 {
        if TcpStream::connect(address).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    return Err("wasmx daemon did not start listening".into());
}

fn post_json(
    address: SocketAddr,
    token: &str,
    path: &str,
    body: &str,
) -> Result<String, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    return Ok(response);
}

fn valid_module(memory_pages: u32) -> Result<Vec<u8>, Box<dyn Error>> {
    let wat = format!(
        "(module (memory (export \"memory\") {memory_pages}) (func (export \"wasmx_main\") (result i32) i32.const 0))"
    );
    return Ok(wat::parse_str(wat)?);
}

fn deploy_body(tenant: &str, deployment: &str, wasm: &[u8]) -> String {
    return format!(
        "{{\"tenant_id\":\"{tenant}\",\"deployment_id\":\"{deployment}\",\"wasm_base64\":\"{}\"}}",
        BASE64.encode(wasm)
    );
}

fn is_success(response: &str) -> bool {
    return response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.1 201");
}

#[cfg(unix)]
#[test]
fn rejects_existing_token_with_group_or_world_access() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_root("weak-token")?;
    let token_path = root.join("token");
    fs::write(&token_path, format!("{}\n", "a".repeat(64)))?;
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o644))?;
    let (_listener, address) = occupied_loopback()?;

    let output = Command::new(env!("CARGO_BIN_EXE_wasmx-desktop-daemon"))
        .env("HOME", &root)
        .env("WASMX_DESKTOP_ADDR", address.to_string())
        .env("WASMX_DESKTOP_TOKEN_FILE", &token_path)
        .env("WASMX_ARTIFACT_ROOT", root.join("artifacts"))
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();

    assert!(!output.status.success());
    assert!(stderr.contains("token"));
    assert!(
        stderr.contains("0600")
            || stderr.contains("permission")
            || stderr.contains("mode")
            || stderr.contains("owner-only")
    );
    fs::remove_dir_all(root)?;
    return Ok(());
}

#[cfg(unix)]
#[test]
fn rejects_symlink_token_path() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let root = temp_root("symlink-token")?;
    let target = root.join("real-token");
    let token_path = root.join("token-link");
    fs::write(&target, format!("{}\n", "b".repeat(64)))?;
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600))?;
    symlink(&target, &token_path)?;
    let (_listener, address) = occupied_loopback()?;

    let output = Command::new(env!("CARGO_BIN_EXE_wasmx-desktop-daemon"))
        .env("HOME", &root)
        .env("WASMX_DESKTOP_ADDR", address.to_string())
        .env("WASMX_DESKTOP_TOKEN_FILE", &token_path)
        .env("WASMX_ARTIFACT_ROOT", root.join("artifacts"))
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();

    assert!(!output.status.success());
    assert!(stderr.contains("token"));
    assert!(
        stderr.contains("symlink") || stderr.contains("regular") || stderr.contains("file type")
    );
    fs::remove_dir_all(root)?;
    return Ok(());
}

#[cfg(unix)]
#[test]
fn deployment_refuses_symlinked_generation_directory() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let root = temp_root("deploy-symlink")?;
    let token_path = root.join("token");
    let token = "0123456789abcdef0123456789abcdef0123456789abcdef";
    secure_token(&token_path, token)?;

    let tenant_root = root.join("artifacts/tenant-a");
    let outside = root.join("outside");
    fs::create_dir_all(&tenant_root)?;
    fs::create_dir_all(&outside)?;
    symlink(&outside, tenant_root.join("deploy-a"))?;

    let address = unused_loopback()?;
    let mut daemon = start_daemon(&root, address, &token_path)?;
    wait_until_listening(address)?;
    let response = post_json(
        address,
        token,
        "/v1/deploy",
        &deploy_body("tenant-a", "deploy-a", &valid_module(1)?),
    )?;
    let _ = daemon.kill();
    let _ = daemon.wait();

    assert!(
        !is_success(&response),
        "deployment followed a symlinked generation directory: {response}"
    );
    assert!(!outside.join("module.wasm").exists());
    fs::remove_dir_all(root)?;
    return Ok(());
}

#[cfg(unix)]
#[test]
fn deployment_refuses_symlinked_module_file() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let root = temp_root("module-symlink")?;
    let token_path = root.join("token");
    let token = "0123456789abcdef0123456789abcdef0123456789abcdef";
    secure_token(&token_path, token)?;

    let deployment_root = root.join("artifacts/tenant-a/deploy-a");
    let outside = root.join("outside-module.wasm");
    fs::create_dir_all(&deployment_root)?;
    fs::write(&outside, b"sentinel")?;
    symlink(&outside, deployment_root.join("module.wasm"))?;

    let address = unused_loopback()?;
    let mut daemon = start_daemon(&root, address, &token_path)?;
    wait_until_listening(address)?;
    let response = post_json(
        address,
        token,
        "/v1/deploy",
        &deploy_body("tenant-a", "deploy-a", &valid_module(1)?),
    )?;
    let _ = daemon.kill();
    let _ = daemon.wait();

    assert!(
        !is_success(&response),
        "deployment followed a symlinked module file: {response}"
    );
    assert_eq!(fs::read(&outside)?, b"sentinel");
    fs::remove_dir_all(root)?;
    return Ok(());
}

#[test]
fn deployment_id_is_immutable_without_explicit_delete() -> Result<(), Box<dyn Error>> {
    let root = temp_root("immutable")?;
    let token_path = root.join("token");
    let token = "0123456789abcdef0123456789abcdef0123456789abcdef";
    secure_token(&token_path, token)?;

    let address = unused_loopback()?;
    let mut daemon = start_daemon(&root, address, &token_path)?;
    wait_until_listening(address)?;

    let first = valid_module(1)?;
    let second = valid_module(2)?;
    let first_response = post_json(
        address,
        token,
        "/v1/deploy",
        &deploy_body("tenant-a", "deploy-a", &first),
    )?;
    assert!(
        is_success(&first_response),
        "initial deployment failed: {first_response}"
    );

    let second_response = post_json(
        address,
        token,
        "/v1/deploy",
        &deploy_body("tenant-a", "deploy-a", &second),
    )?;

    let stored = fs::read(root.join("artifacts/tenant-a/deploy-a/module.wasm"))?;
    let _ = daemon.kill();
    let _ = daemon.wait();

    assert!(
        !is_success(&second_response),
        "same deployment identity accepted different module bytes: {second_response}"
    );
    assert_eq!(Sha256::digest(&stored), Sha256::digest(&first));
    fs::remove_dir_all(root)?;
    return Ok(());
}

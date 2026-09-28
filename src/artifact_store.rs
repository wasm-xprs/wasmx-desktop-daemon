use anyhow::{Context as _, Result, anyhow, bail};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub async fn ensure_root(root: &Path) -> Result<()> {
    tokio::fs::create_dir_all(root)
        .await
        .with_context(|| format!("could not create artifact root {}", root.display()))?;
    validate_real_directory(root, "artifact root").await?;
    return Ok(());
}

pub async fn write_immutable(
    root: &Path,
    tenant_id: &str,
    deployment_id: &str,
    bytes: &[u8],
) -> Result<PathBuf> {
    let deployment = validate_deployment_directory(root, tenant_id, deployment_id, true).await?;
    let path = deployment.join("module.wasm");

    match tokio::fs::symlink_metadata(&path).await {
        Ok(metadata) => {
            require_regular_file(&path, &metadata)?;
            let existing = tokio::fs::read(&path).await?;
            if existing == bytes {
                return Ok(path);
            }
            bail!("deployment id already exists with different module bytes");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not inspect deployment module {}", path.display()));
        }
    }

    let temporary = deployment.join(format!(".module-{}.tmp", Uuid::new_v4().simple()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await
        .with_context(|| format!("could not create {}", temporary.display()))?;
    if let Err(error) = file.write_all(bytes).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).context("could not write temporary deployment module");
    }
    if let Err(error) = file.sync_all().await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).context("could not sync temporary deployment module");
    }
    drop(file);

    // Re-admit the directory after the write and immediately before publish.
    // The hard-link is intentionally no-overwrite: unlike rename, it cannot
    // silently replace an immutable deployment identity.
    validate_deployment_directory(root, tenant_id, deployment_id, false).await?;
    match tokio::fs::hard_link(&temporary, &path).await {
        Ok(()) => {
            tokio::fs::remove_file(&temporary).await?;
            return Ok(path);
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = tokio::fs::remove_file(&temporary).await;
            let metadata = tokio::fs::symlink_metadata(&path).await?;
            require_regular_file(&path, &metadata)?;
            let existing = tokio::fs::read(&path).await?;
            if existing == bytes {
                return Ok(path);
            }
            bail!("deployment id already exists with different module bytes");
        }
        Err(error) => {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error).context("could not atomically publish deployment module");
        }
    }
}

pub async fn read_module(root: &Path, tenant_id: &str, deployment_id: &str) -> Result<Vec<u8>> {
    let deployment = validate_deployment_directory(root, tenant_id, deployment_id, false).await?;
    let path = deployment.join("module.wasm");
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .with_context(|| format!("deployment artifact not found: {}", path.display()))?;
    require_regular_file(&path, &metadata)?;
    let bytes = tokio::fs::read(&path).await?;
    return Ok(bytes);
}

pub async fn remove_module(root: &Path, tenant_id: &str, deployment_id: &str) -> Result<()> {
    let deployment = validate_deployment_directory(root, tenant_id, deployment_id, false).await?;
    let path = deployment.join("module.wasm");
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .with_context(|| format!("deployment artifact not found: {}", path.display()))?;
    require_regular_file(&path, &metadata)?;
    tokio::fs::remove_file(&path).await?;
    return Ok(());
}

async fn validate_deployment_directory(
    root: &Path,
    tenant_id: &str,
    deployment_id: &str,
    create: bool,
) -> Result<PathBuf> {
    validate_path_component(tenant_id)?;
    validate_path_component(deployment_id)?;
    validate_real_directory(root, "artifact root").await?;
    let canonical_root = tokio::fs::canonicalize(root)
        .await
        .context("could not resolve artifact root")?;

    let tenant = root.join(tenant_id);
    if create {
        ensure_real_directory(&tenant, "tenant deployment directory").await?;
    } else {
        validate_real_directory(&tenant, "tenant deployment directory").await?;
    }

    let deployment = tenant.join(deployment_id);
    if create {
        ensure_real_directory(&deployment, "deployment generation directory").await?;
    } else {
        validate_real_directory(&deployment, "deployment generation directory").await?;
    }

    let canonical_deployment = tokio::fs::canonicalize(&deployment)
        .await
        .context("could not resolve deployment generation directory")?;
    if !canonical_deployment.starts_with(&canonical_root) {
        bail!("deployment generation directory escapes artifact root");
    }
    return Ok(deployment);
}

async fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    match tokio::fs::create_dir(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not create {label} {}", path.display()));
        }
    }
    return validate_real_directory(path, label).await;
}

async fn validate_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("could not inspect {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} {} must be a real directory", path.display());
    }
    return Ok(());
}

fn require_regular_file(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("deployment module {} must be a regular file", path.display());
    }
    return Ok(());
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
        bail!("invalid deployment path component");
    }
    return Ok(());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_components_fail_closed() {
        assert!(validate_path_component("tenant-a").is_ok());
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component("tenant/escape").is_err());
        return;
    }
}

#![allow(clippy::needless_return)]

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;

const DISTRIBUTION_SCHEMA: &str = "ores.common-desktop.rust-distribution/v1";
const PACKAGE_MANAGER: &str = "zed";
const CONSUMER_PATH_POLICY: &str = "materialized_root_plus_crate_path";
const CONSUMER_INSTALL_MODE: &str = "copy";
const CONSUMER_ADAPTER: &str = "none";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ZedAuthority {
    org: String,
    name: String,
    version: String,
    install_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedTransport {
    coordinate: String,
    version: String,
    materialized_root: String,
    crate_paths: Vec<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("zed distribution validation failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let distribution_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "distribution/rust-packages.json".to_owned());
    let zpkg_path = env::args()
        .nth(2)
        .unwrap_or_else(|| ".zpkg.toml".to_owned());

    let distribution_text = fs::read_to_string(&distribution_path)
        .map_err(|error| format!("cannot read {distribution_path}: {error}"))?;
    let distribution: Value = serde_json::from_str(&distribution_text)
        .map_err(|error| format!("invalid JSON in {distribution_path}: {error}"))?;
    let zpkg_text = fs::read_to_string(&zpkg_path)
        .map_err(|error| format!("cannot read {zpkg_path}: {error}"))?;

    let validated = validate_zed_transport(&distribution, &zpkg_text)?;
    println!(
        "validated Zed transport {}@{} at {} with {} Rust crate paths",
        validated.coordinate,
        validated.version,
        validated.materialized_root,
        validated.crate_paths.len()
    );
    return Ok(());
}

fn validate_zed_transport(
    distribution: &Value,
    zpkg_text: &str,
) -> Result<ValidatedTransport, String> {
    if distribution.get("schema").and_then(Value::as_str) != Some(DISTRIBUTION_SCHEMA) {
        return Err(format!(
            "distribution schema must remain {DISTRIBUTION_SCHEMA}"
        ));
    }

    let authority = parse_zed_authority(zpkg_text)?;
    validate_slug(&authority.org, "package.org")?;
    validate_slug(&authority.name, "package.name")?;
    if authority.version.trim().is_empty() {
        return Err("package.version must be non-empty".to_owned());
    }
    if !normalized_relative_path(&authority.install_dir) {
        return Err(format!(
            "install.dir is not a normalized relative path: {}",
            authority.install_dir
        ));
    }

    let transport = distribution
        .get("package_transport")
        .ok_or_else(|| "distribution manifest is missing package_transport object".to_owned())?;
    if !transport.is_object() {
        return Err("distribution package_transport must be an object".to_owned());
    }

    require_transport_string(transport, "manager", PACKAGE_MANAGER)?;
    require_transport_string(transport, "consumer_path_policy", CONSUMER_PATH_POLICY)?;
    require_transport_string(transport, "consumer_install_mode", CONSUMER_INSTALL_MODE)?;
    require_transport_string(transport, "consumer_adapter", CONSUMER_ADAPTER)?;
    if transport
        .get("frozen_lock_required")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err("package_transport.frozen_lock_required must remain true".to_owned());
    }

    let coordinate = format!("{}/{}", authority.org, authority.name);
    let declared_coordinate = required_string(
        transport,
        "coordinate",
        "distribution package_transport",
    )?;
    if declared_coordinate != coordinate {
        return Err(format!(
            "package_transport.coordinate must match .zpkg.toml package identity: expected {coordinate}, got {declared_coordinate}"
        ));
    }

    let declared_version = required_string(transport, "version", "distribution package_transport")?;
    if declared_version != authority.version {
        return Err(format!(
            "package_transport.version must match .zpkg.toml package.version: expected {}, got {declared_version}",
            authority.version
        ));
    }
    if distribution.get("version").and_then(Value::as_str) != Some(authority.version.as_str()) {
        return Err(format!(
            "distribution version must match .zpkg.toml package.version {}",
            authority.version
        ));
    }

    let declared_install_dir = required_string(
        transport,
        "install_dir",
        "distribution package_transport",
    )?;
    if declared_install_dir != authority.install_dir {
        return Err(format!(
            "package_transport.install_dir must match .zpkg.toml install.dir: expected {}, got {declared_install_dir}",
            authority.install_dir
        ));
    }

    let expected_root = format!("{}/{}/{}", authority.install_dir, authority.org, authority.name);
    let declared_root = required_string(
        transport,
        "materialized_root",
        "distribution package_transport",
    )?;
    if !normalized_relative_path(declared_root) {
        return Err(format!(
            "package_transport.materialized_root is not a normalized relative path: {declared_root}"
        ));
    }
    if declared_root != expected_root {
        return Err(format!(
            "package_transport.materialized_root must be install_dir/org/name: expected {expected_root}, got {declared_root}"
        ));
    }

    let crates = distribution
        .get("crates")
        .and_then(Value::as_array)
        .ok_or_else(|| "distribution manifest is missing crates array".to_owned())?;
    if crates.is_empty() {
        return Err("distribution crates array must not be empty".to_owned());
    }

    let mut declared_paths = BTreeSet::new();
    let mut crate_paths = Vec::with_capacity(crates.len());
    for row in crates {
        let path = required_string(row, "path", "distribution crate")?;
        if !normalized_relative_path(path) {
            return Err(format!("invalid distribution crate path: {path}"));
        }
        if !declared_paths.insert(path.to_owned()) {
            return Err(format!("duplicate distribution crate path: {path}"));
        }
        crate_paths.push(format!("{expected_root}/{path}"));
    }

    return Ok(ValidatedTransport {
        coordinate,
        version: authority.version,
        materialized_root: expected_root,
        crate_paths,
    });
}

fn parse_zed_authority(text: &str) -> Result<ZedAuthority, String> {
    let mut section = String::new();
    let mut values = BTreeMap::<String, String>::new();

    for (index, raw_line) in text.lines().enumerate() {
        let line_number = index.saturating_add(1);
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') {
            if !line.ends_with(']') || line.len() < 3 {
                return Err(format!("invalid TOML section header on line {line_number}"));
            }
            section = line[1..line.len() - 1].trim().to_owned();
            continue;
        }
        if section != "package" && section != "install" {
            continue;
        }

        let (raw_key, raw_value) = line.split_once('=').ok_or_else(|| {
            format!("invalid key/value syntax in [{section}] on line {line_number}")
        })?;
        let key = raw_key.trim();
        let authority_key = match (section.as_str(), key) {
            ("package", "org") => Some("package.org"),
            ("package", "name") => Some("package.name"),
            ("package", "version") => Some("package.version"),
            ("install", "dir") => Some("install.dir"),
            _ => None,
        };
        let Some(authority_key) = authority_key else {
            continue;
        };
        let value = parse_simple_quoted_string(raw_value, line_number, authority_key)?;
        if values.insert(authority_key.to_owned(), value).is_some() {
            return Err(format!("duplicate authority key {authority_key}"));
        }
    }

    return Ok(ZedAuthority {
        org: take_required(&mut values, "package.org")?,
        name: take_required(&mut values, "package.name")?,
        version: take_required(&mut values, "package.version")?,
        install_dir: take_required(&mut values, "install.dir")?,
    });
}

fn parse_simple_quoted_string(
    raw_value: &str,
    line_number: usize,
    key: &str,
) -> Result<String, String> {
    let value = raw_value.trim();
    let Some(remainder) = value.strip_prefix('"') else {
        return Err(format!(
            "{key} must be a basic quoted TOML string on line {line_number}"
        ));
    };
    let Some(closing) = remainder.find('"') else {
        return Err(format!("unterminated {key} string on line {line_number}"));
    };
    let body = &remainder[..closing];
    if body.contains('\\') {
        return Err(format!(
            "{key} may not use TOML escapes in distribution authority fields"
        ));
    }
    let trailing = remainder[closing + 1..].trim();
    if !trailing.is_empty() && !trailing.starts_with('#') {
        return Err(format!(
            "unexpected trailing content after {key} on line {line_number}"
        ));
    }
    if body.is_empty() {
        return Err(format!("{key} must be non-empty"));
    }
    return Ok(body.to_owned());
}

fn take_required(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    return values
        .remove(key)
        .ok_or_else(|| format!(".zpkg.toml is missing authority field {key}"));
}

fn require_transport_string(
    transport: &Value,
    key: &str,
    expected: &str,
) -> Result<(), String> {
    let actual = required_string(transport, key, "distribution package_transport")?;
    if actual != expected {
        return Err(format!(
            "package_transport.{key} must remain {expected}, got {actual}"
        ));
    }
    return Ok(());
}

fn required_string<'a>(value: &'a Value, key: &str, context: &str) -> Result<&'a str, String> {
    return value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{context} is missing string field {key}"));
}

fn validate_slug(value: &str, label: &str) -> Result<(), String> {
    let valid = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid {
        return Err(format!(
            "{label} must use lowercase ASCII letters, digits, or hyphens: {value}"
        ));
    }
    return Ok(());
}

fn normalized_relative_path(value: &str) -> bool {
    return !value.is_empty()
        && !value.starts_with('/')
        && !value.contains('\\')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn zpkg() -> &'static str {
        return r#"
[package]
org = "oresoftware"
name = "ores-common-desktop-infra"
version = "0.1.0"
description = "fixture"

[package.repository]
vcs = "git"
url = "https://github.com/ORESoftware/ores-common-desktop-infra"

[install]
dir = "zed_modules"

[targets.runtime]
dir = "runtime/rust"
adapter = "none"
"#;
    }

    fn distribution() -> Value {
        return json!({
            "schema": DISTRIBUTION_SCHEMA,
            "version": "0.1.0",
            "crates": [
                {"path": "infra/rust", "publish_required": true},
                {"path": "runtime/rust", "publish_required": true}
            ],
            "package_transport": {
                "manager": PACKAGE_MANAGER,
                "coordinate": "oresoftware/ores-common-desktop-infra",
                "version": "0.1.0",
                "install_dir": "zed_modules",
                "materialized_root": "zed_modules/oresoftware/ores-common-desktop-infra",
                "consumer_path_policy": CONSUMER_PATH_POLICY,
                "consumer_install_mode": CONSUMER_INSTALL_MODE,
                "consumer_adapter": CONSUMER_ADAPTER,
                "frozen_lock_required": true
            }
        });
    }

    #[test]
    fn accepts_transport_and_derives_all_crate_paths() {
        let validated = validate_zed_transport(&distribution(), zpkg()).unwrap();
        assert_eq!(
            validated.coordinate,
            "oresoftware/ores-common-desktop-infra"
        );
        assert_eq!(validated.version, "0.1.0");
        assert_eq!(
            validated.materialized_root,
            "zed_modules/oresoftware/ores-common-desktop-infra"
        );
        assert_eq!(
            validated.crate_paths,
            vec![
                "zed_modules/oresoftware/ores-common-desktop-infra/infra/rust",
                "zed_modules/oresoftware/ores-common-desktop-infra/runtime/rust"
            ]
        );
    }

    #[test]
    fn rejects_coordinate_drift() {
        let mut distribution = distribution();
        distribution["package_transport"]["coordinate"] = json!("other/package");
        let error = validate_zed_transport(&distribution, zpkg()).unwrap_err();
        assert!(error.contains("coordinate must match"));
    }

    #[test]
    fn rejects_version_drift() {
        let mut distribution = distribution();
        distribution["package_transport"]["version"] = json!("0.2.0");
        let error = validate_zed_transport(&distribution, zpkg()).unwrap_err();
        assert!(error.contains("version must match"));
    }

    #[test]
    fn rejects_materialized_root_drift() {
        let mut distribution = distribution();
        distribution["package_transport"]["materialized_root"] =
            json!("vendor/oresoftware/ores-common-desktop-infra");
        let error = validate_zed_transport(&distribution, zpkg()).unwrap_err();
        assert!(error.contains("must be install_dir/org/name"));
    }

    #[test]
    fn rejects_non_frozen_consumer_policy() {
        let mut distribution = distribution();
        distribution["package_transport"]["frozen_lock_required"] = json!(false);
        let error = validate_zed_transport(&distribution, zpkg()).unwrap_err();
        assert_eq!(
            error,
            "package_transport.frozen_lock_required must remain true"
        );
    }

    #[test]
    fn rejects_install_mode_drift() {
        let mut distribution = distribution();
        distribution["package_transport"]["consumer_install_mode"] = json!("symlink");
        let error = validate_zed_transport(&distribution, zpkg()).unwrap_err();
        assert!(error.contains("consumer_install_mode must remain copy"));
    }

    #[test]
    fn rejects_crate_path_traversal() {
        let mut distribution = distribution();
        distribution["crates"][0]["path"] = json!("../private");
        let error = validate_zed_transport(&distribution, zpkg()).unwrap_err();
        assert_eq!(error, "invalid distribution crate path: ../private");
    }

    #[test]
    fn rejects_zpkg_install_dir_drift() {
        let changed = zpkg().replace("dir = \"zed_modules\"", "dir = \"vendor\"");
        let error = validate_zed_transport(&distribution(), &changed).unwrap_err();
        assert!(error.contains("install_dir must match"));
    }

    #[test]
    fn parser_rejects_duplicate_authority_key() {
        let changed = zpkg().replace(
            "name = \"ores-common-desktop-infra\"",
            "name = \"ores-common-desktop-infra\"\nname = \"duplicate\"",
        );
        let error = parse_zed_authority(&changed).unwrap_err();
        assert_eq!(error, "duplicate authority key package.name");
    }
}

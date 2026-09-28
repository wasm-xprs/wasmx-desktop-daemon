from pathlib import Path

path = Path("src/main.rs")
text = path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    text = text.replace(old, new, 1)


replace_once("mod ores_adapter;", "mod ores_adapter;\nmod ores_receipt;", "module declaration")
replace_once(
    '''use ores_adapter::{\n    OresDeploymentProvenance, OresLambdaAdapterV1, WASMX_GUEST_ABI, WASMX_TARGET_TRIPLE,\n};''',
    '''use ores_adapter::{\n    OresDeploymentProvenance, OresLambdaAdapterV1, WASMX_GUEST_ABI, WASMX_TARGET_TRIPLE,\n};\nuse ores_receipt::OresBuildEvidence;''',
    "receipt import",
)
replace_once(
    '''    #[serde(default)]\n    ores_adapter: Option<OresLambdaAdapterV1>,\n}''',
    '''    #[serde(default)]\n    ores_adapter: Option<OresLambdaAdapterV1>,\n    #[serde(default)]\n    ores_adapter_raw_base64: Option<String>,\n    #[serde(default)]\n    ores_receipt_raw_base64: Option<String>,\n}''',
    "deploy request evidence fields",
)
replace_once(
    '''    compiled: bool,\n    ores_adapter_verified: bool,\n}''',
    '''    compiled: bool,\n    ores_adapter_verified: bool,\n    ores_receipt_verified: bool,\n}''',
    "deploy response receipt state",
)
replace_once(
    '''    #[serde(default, skip_serializing_if = "Option::is_none")]\n    ores_provenance: Option<OresDeploymentProvenance>,\n}''',
    '''    #[serde(default, skip_serializing_if = "Option::is_none")]\n    ores_provenance: Option<OresDeploymentProvenance>,\n    #[serde(default, skip_serializing_if = "Option::is_none")]\n    ores_build_evidence: Option<OresBuildEvidence>,\n}''',
    "manifest receipt evidence",
)
replace_once(
    '''    ores_adapter_sha256: Option<String>,\n    ores_source: Option<String>,\n    ores_source_sha256: Option<String>,\n}''',
    '''    ores_adapter_sha256: Option<String>,\n    ores_source: Option<String>,\n    ores_source_sha256: Option<String>,\n    ores_build_receipt_bound: bool,\n    ores_receipt_sha256: Option<String>,\n    ores_adapter_raw_sha256: Option<String>,\n}''',
    "summary receipt evidence",
)
replace_once(
    '''    if bytes.is_empty() || bytes.len() > MAX_MODULE_BYTES {\n        return Err((\n            StatusCode::BAD_REQUEST,\n            format!("module must be between 1 and {MAX_MODULE_BYTES} bytes"),\n        ));\n    }\n\n    let module = compile_module''',
    '''    if bytes.is_empty() || bytes.len() > MAX_MODULE_BYTES {\n        return Err((\n            StatusCode::BAD_REQUEST,\n            format!("module must be between 1 and {MAX_MODULE_BYTES} bytes"),\n        ));\n    }\n\n    let ores_build_evidence = match (\n        request.ores_adapter.as_ref(),\n        request.ores_adapter_raw_base64.as_deref(),\n        request.ores_receipt_raw_base64.as_deref(),\n    ) {\n        (Some(adapter), Some(raw_adapter), Some(raw_receipt)) => {\n            let raw_adapter = BASE64.decode(raw_adapter.as_bytes()).map_err(|_| {\n                (\n                    StatusCode::BAD_REQUEST,\n                    "ores_adapter_raw_base64 is not valid base64".to_owned(),\n                )\n            })?;\n            let raw_receipt = BASE64.decode(raw_receipt.as_bytes()).map_err(|_| {\n                (\n                    StatusCode::BAD_REQUEST,\n                    "ores_receipt_raw_base64 is not valid base64".to_owned(),\n                )\n            })?;\n            Some(ores_receipt::verify(adapter, &raw_adapter, &raw_receipt, &bytes).map_err(\n                |error| {\n                    (\n                        StatusCode::BAD_REQUEST,\n                        format!("ORES WASM receipt validation failed: {error}"),\n                    )\n                },\n            )?)\n        }\n        (_, None, None) => None,\n        _ => {\n            return Err((\n                StatusCode::BAD_REQUEST,\n                "ORES receipt evidence requires ores_adapter plus exact raw adapter and receipt bytes"\n                    .to_owned(),\n            ));\n        }\n    };\n\n    let module = compile_module''',
    "server receipt verification",
)
replace_once(
    '''        ores_adapter_verified,\n        ores_provenance,\n    };''',
    '''        ores_adapter_verified,\n        ores_provenance,\n        ores_build_evidence: ores_build_evidence.clone(),\n    };''',
    "manifest build evidence assignment",
)
replace_once(
    '''        compiled: true,\n        ores_adapter_verified,\n    }));''',
    '''        compiled: true,\n        ores_adapter_verified,\n        ores_receipt_verified: ores_build_evidence.is_some(),\n    }));''',
    "deploy response evidence state",
)
replace_once(
    '''        (None, _) => {}\n    }\n    Ok(())''',
    '''        (None, _) => {}\n    }\n    if let Some(evidence) = manifest.ores_build_evidence.as_ref() {\n        evidence.validate()?;\n        if !manifest.ores_adapter_verified || manifest.ores_provenance.is_none() {\n            bail!("ORES build evidence requires verified adapter provenance");\n        }\n        if evidence.artifact_sha256 != manifest.sha256 {\n            bail!("ORES build evidence artifact digest does not match deployment manifest");\n        }\n    }\n    Ok(())''',
    "manifest build evidence validation",
)
replace_once(
    '''    Ok(DeploymentSummary {\n        tenant_id: key.tenant_id.clone(),''',
    '''    let (\n        ores_build_receipt_bound,\n        ores_receipt_sha256,\n        ores_adapter_raw_sha256,\n    ) = match manifest.ores_build_evidence.as_ref() {\n        Some(evidence) => (\n            true,\n            Some(evidence.receipt_sha256.clone()),\n            Some(evidence.adapter_raw_sha256.clone()),\n        ),\n        None => (false, None, None),\n    };\n\n    Ok(DeploymentSummary {\n        tenant_id: key.tenant_id.clone(),''',
    "summary receipt projection setup",
)
replace_once(
    '''        ores_source,\n        ores_source_sha256,\n    })''',
    '''        ores_source,\n        ores_source_sha256,\n        ores_build_receipt_bound,\n        ores_receipt_sha256,\n        ores_adapter_raw_sha256,\n    })''',
    "summary receipt projection fields",
)

# Every existing test/legacy manifest constructor should explicitly carry no receipt evidence.
text = text.replace("            ores_provenance: None,\n", "            ores_provenance: None,\n            ores_build_evidence: None,\n")
# The deploy manifest constructor uses shorthand provenance.
# The replacement above intentionally does not match it; it was handled explicitly.

path.write_text(text)

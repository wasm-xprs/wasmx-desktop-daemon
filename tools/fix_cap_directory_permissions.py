from pathlib import Path

path = Path("src/main.rs")
text = path.read_text()


def replace_once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one match, found {count}: {old[:120]!r}")
    text = text.replace(old, new, 1)


replace_once(
'''fn harden_cap_directory_permissions(directory: &Dir) -> Result<()> {\n    #[cfg(unix)]\n    {\n        use std::os::unix::fs::PermissionsExt as _;\n        directory\n            .try_clone()?\n            .into_std_file()\n            .set_permissions(std::fs::Permissions::from_mode(0o700))?;\n    }\n    Ok(())\n}\n\n''',
'''fn harden_child_directory_permissions(parent: &Dir, child: &str) -> Result<()> {\n    validate_path_component(child)?;\n    #[cfg(unix)]\n    {\n        use std::os::unix::fs::PermissionsExt as _;\n        parent.set_permissions(child, std::fs::Permissions::from_mode(0o700))?;\n    }\n    Ok(())\n}\n\n'''
)

replace_once(
'''            let directory = root.open_dir_nofollow(tenant_id).with_context(|| {\n                format!("tenant artifact directory is not a real directory: {tenant_id}")\n            })?;\n            harden_cap_directory_permissions(&directory)?;\n            Ok(directory)''',
'''            harden_child_directory_permissions(root, tenant_id)?;\n            let directory = root.open_dir_nofollow(tenant_id).with_context(|| {\n                format!("tenant artifact directory is not a real directory: {tenant_id}")\n            })?;\n            Ok(directory)'''
)

replace_once(
'''    let deployment = tenant.open_dir_nofollow(deployment_id).with_context(|| {\n        format!("deployment directory is not a real directory: {deployment_id}")\n    })?;\n    harden_cap_directory_permissions(&deployment)?;\n    Ok(deployment)''',
'''    harden_child_directory_permissions(tenant, deployment_id)?;\n    let deployment = tenant.open_dir_nofollow(deployment_id).with_context(|| {\n        format!("deployment directory is not a real directory: {deployment_id}")\n    })?;\n    Ok(deployment)'''
)

path.write_text(text)

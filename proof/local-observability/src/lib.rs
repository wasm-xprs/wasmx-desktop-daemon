#![forbid(unsafe_code)]

use next_loggers::desktop::DesktopLocalFile;

pub fn open_local_transport(app_name: &str) -> Result<(), next_loggers::LoggerError> {
    let transport = DesktopLocalFile::new(app_name)?;
    drop(transport);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_logging_dependency_exposes_desktop_transport() {
        let root = std::env::temp_dir().join(format!(
            "ores-local-observability-proof-{}",
            std::process::id()
        ));
        std::env::set_var("ORES_LOCAL_LOG_ROOT", &root);
        open_local_transport("funded-proof").expect("desktop local transport");
        let _ = std::fs::remove_dir_all(root);
    }
}

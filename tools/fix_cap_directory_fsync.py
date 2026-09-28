from pathlib import Path

path = Path("src/main.rs")
text = path.read_text()
old = '''        #[cfg(unix)]
        directory.try_clone()?.into_std_file().sync_all()?;
        Ok(())
'''
new = '''        #[cfg(unix)]
        sync_cap_directory(&directory)?;
        Ok(())
'''
if text.count(old) != 1:
    raise SystemExit(f"expected one directory sync site, found {text.count(old)}")
text = text.replace(old, new, 1)
anchor = '''async fn read_regular_file_no_symlink_at(
'''
helper = '''#[cfg(unix)]
fn sync_cap_directory(directory: &Dir) -> Result<()> {
    // `open_dir_nofollow` may retain an O_PATH-style capability on Linux.
    // Re-open `.` relative to that capability as a syncable directory file
    // instead of converting the O_PATH handle itself and calling fsync on it.
    let mut options = CapOpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.maybe_dir(true);
    let file = directory.open_with(".", &options)?;
    file.sync_all()?;
    return Ok(());
}

async fn read_regular_file_no_symlink_at(
'''
if text.count(anchor) != 1:
    raise SystemExit(f"expected one read helper anchor, found {text.count(anchor)}")
text = text.replace(anchor, helper, 1)
path.write_text(text)

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn write(path: &Path, raw: &[u8]) -> io::Result<()> {
    write_impl(path, raw, true)
}

pub(crate) fn write_ephemeral(path: &Path, raw: &[u8]) -> io::Result<()> {
    write_impl(path, raw, false)
}

fn write_impl(path: &Path, raw: &[u8], durable: bool) -> io::Result<()> {
    let destination = path.to_path_buf();
    let directory = destination.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(directory)?;
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "atomic file destination must be a regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let (temporary_path, mut temporary) = create_temporary_file(directory, &destination)?;
    let result: io::Result<()> = (|| {
        temporary.write_all(raw)?;
        if durable {
            temporary.sync_all()?;
        }
        drop(temporary);
        atomic_replace(&temporary_path, &destination, durable)?;
        if durable {
            sync_parent_directory(&destination)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn create_temporary_file(directory: &Path, destination: &Path) -> io::Result<(PathBuf, File)> {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("runtime");
    for _ in 0..1_024 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            sequence
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "atomic temporary file namespace is exhausted",
    ))
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path, _durable: bool) -> io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path, durable: bool) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: Both path buffers are NUL-terminated and remain alive for the call.
    let replaced = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | if durable { MOVEFILE_WRITE_THROUGH } else { 0 },
        )
    };
    if replaced == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{write, write_ephemeral};

    #[test]
    fn repeated_writes_replace_the_existing_file() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("runtime.json");
        write(&path, b"first").expect("first atomic write");
        write(&path, b"second").expect("replacement atomic write");
        assert_eq!(std::fs::read(path).expect("runtime file"), b"second");
    }

    #[test]
    fn non_regular_destinations_are_rejected() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let error = write(directory.path(), b"data").expect_err("directory destination rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn ephemeral_writes_still_replace_atomically() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("preview.jpg");
        write_ephemeral(&path, b"first").expect("first preview");
        write_ephemeral(&path, b"second").expect("replacement preview");
        assert_eq!(std::fs::read(path).expect("preview file"), b"second");
    }
}

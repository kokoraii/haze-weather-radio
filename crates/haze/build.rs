fn main() {
    use std::env;
    use std::fmt::Write as _;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    println!("cargo:rerun-if-env-changed=HAZE_EMBED_GO_SERVICES");
    println!("cargo:rerun-if-changed=../../services/go/go.mod");
    println!("cargo:rerun-if-changed=../../services/go/go.sum");
    println!("cargo:rerun-if-changed=../../services/go/cmd");
    println!("cargo:rerun-if-changed=../../services/go/internal");
    println!("cargo:rerun-if-changed=../../config.yaml");
    println!("cargo:rerun-if-changed=../../bundle");
    println!("cargo:rerun-if-changed=../../bundle/webroot/favicon.ico");
    println!("cargo:rerun-if-changed=../../bundle/webroot/favicon.svg");
    println!("cargo:rerun-if-changed=../../managed");
    println!("cargo:rerun-if-changed=../../webroot");
    println!("cargo:rerun-if-changed=../../audio");

    generate_windows_resources();
    generate_go_assets();
    generate_default_assets();

    fn repo_root() -> Option<PathBuf> {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").ok().map(PathBuf::from)?;
        Some(
            manifest_dir
                .parent()
                .and_then(Path::parent)
                .map(Path::to_path_buf)
                .unwrap_or(manifest_dir),
        )
    }

    fn generate_go_assets() {
        let out_dir = match env::var("OUT_DIR") {
            Ok(value) => PathBuf::from(value),
            Err(_) => return,
        };

        let Some(repo_root) = repo_root() else {
            return;
        };

        let embed_enabled = env::var("HAZE_EMBED_GO_SERVICES")
            .map(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
            .unwrap_or(true);

        let mut binaries: Vec<(String, PathBuf)> = Vec::new();
        if embed_enabled {
            let go_root = repo_root.join("services").join("go");
            if go_root.join("go.mod").is_file() {
                let opus = go_opus_build(&repo_root);
                for (name, package) in go_services() {
                    let output = out_dir.join(executable_name(name));
                    let _ = std::fs::remove_file(&output);
                    let go_cache = out_dir.join("go-build-cache");
                    let go_tmp = out_dir.join("go-tmp");
                    let _ = std::fs::create_dir_all(&go_cache);
                    let _ = std::fs::create_dir_all(&go_tmp);
                    let mut command = Command::new("go");
                    command
                        .current_dir(&go_root)
                        .env("GOCACHE", &go_cache)
                        .env("GOTMPDIR", &go_tmp);
                    if let Some(opus) = &opus {
                        for (key, value) in &opus.env {
                            command.env(key, value);
                        }
                    }
                    command.args(["build", "-trimpath", "-ldflags=-s -w"]);
                    if name == "haze-web" {
                        if opus.is_some() {
                            command.args(["-tags", "opus_cgo"]);
                        } else if require_opus() {
                            println!(
                                "cargo:warning=skipping embedded haze-web; native Opus build inputs were not found"
                            );
                            continue;
                        }
                    }
                    command.arg("-o").arg(&output).arg(package);
                    match command.output() {
                        Ok(result) if result.status.success() && output.is_file() => {
                            binaries.push((executable_name(name).to_string(), output));
                        }
                        Ok(result) => {
                            let stderr = String::from_utf8_lossy(&result.stderr);
                            let stdout = String::from_utf8_lossy(&result.stdout);
                            println!(
                                "cargo:warning=skipping embedded Go service {name}; go build exited with {} stdout={} stderr={}",
                                result.status,
                                stdout.trim(),
                                stderr.trim()
                            );
                        }
                        Err(err) => {
                            println!(
                                "cargo:warning=skipping embedded Go service {name}; unable to run go: {err}"
                            );
                        }
                    }
                }
                if let Some(opus) = &opus {
                    for dll in &opus.runtime_libraries {
                        if dll.is_file() {
                            if let Some(name) = dll.file_name().and_then(|name| name.to_str()) {
                                binaries.push((name.to_string(), dll.clone()));
                            }
                        }
                    }
                }
                for library in go_sherpa_runtime_libraries(&go_root) {
                    if let Some(name) = library.file_name().and_then(|name| name.to_str()) {
                        binaries.push((name.to_string(), library));
                    }
                }
            }
        }

        let mut generated =
            String::from("pub(crate) static EMBEDDED_GO_BINARIES: &[(&str, &[u8])] = &[\n");
        for (name, path) in binaries {
            let literal = format!("{:?}", path.to_string_lossy());
            let _ = writeln!(generated, "    ({name:?}, include_bytes!({literal})),");
        }
        generated.push_str("];\n");
        let _ = std::fs::write(out_dir.join("go_assets.rs"), generated);
    }

    fn generate_windows_resources() {
        if env::var("CARGO_CFG_TARGET_OS").ok().as_deref() != Some("windows") {
            return;
        }

        let Some(repo_root) = repo_root() else {
            return;
        };
        let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
        let icon_path = asset_source_dir(&repo_root, "webroot").join("favicon.ico");

        let mut resource = winresource::WindowsResource::new();
        resource.set("ProductName", "Haze Weather Radio");
        resource.set("FileDescription", "Haze Weather Radio host");
        resource.set("CompanyName", "Haze Weather Radio");
        resource.set(
            "LegalCopyright",
            "Copyright (c) 2026 Haze Weather Radio contributors",
        );
        resource.set("OriginalFilename", "haze.exe");
        resource.set("InternalName", "haze");
        resource.set("ProductVersion", &version);
        resource.set("FileVersion", &version);
        resource.set(
            "Comments",
            "Starts and supervises the Haze Weather Radio services.",
        );
        if icon_path.is_file() {
            resource.set_icon(icon_path.to_string_lossy().as_ref());
        } else {
            println!(
                "cargo:warning=daemon icon source not found at {}; build will use the default executable icon",
                icon_path.display()
            );
        }
        if let Err(err) = resource.compile() {
            println!("cargo:warning=failed to compile haze.exe Windows resources: {err}");
        }
    }

    fn generate_default_assets() {
        let out_dir = match env::var("OUT_DIR") {
            Ok(value) => PathBuf::from(value),
            Err(_) => return,
        };
        let Some(repo_root) = repo_root() else {
            let _ = std::fs::write(
                out_dir.join("default_assets.rs"),
                "pub(crate) static DEFAULT_ASSETS: &[(&str, &[u8])] = &[];\n",
            );
            return;
        };

        let mut files = Vec::new();
        collect_default_file(&repo_root, "config.yaml", &mut files);
        collect_default_dir(
            &asset_source_dir(&repo_root, "managed"),
            "managed",
            &mut files,
        );
        collect_default_dir(
            &asset_source_dir(&repo_root, "webroot"),
            "webroot",
            &mut files,
        );
        collect_default_dir(&asset_source_dir(&repo_root, "audio"), "audio", &mut files);
        files.sort_by(|left, right| left.0.cmp(&right.0));

        let mut generated =
            String::from("pub(crate) static DEFAULT_ASSETS: &[(&str, &[u8])] = &[\n");
        for (relative, path) in files {
            let literal = format!("{:?}", path.to_string_lossy());
            let _ = writeln!(generated, "    ({relative:?}, include_bytes!({literal})),");
        }
        generated.push_str("];\n");
        let _ = std::fs::write(out_dir.join("default_assets.rs"), generated);
    }

    fn collect_default_file(repo_root: &Path, relative: &str, files: &mut Vec<(String, PathBuf)>) {
        let path = repo_root.join(relative);
        if path.is_file() && include_default_asset(relative) {
            files.push((relative.replace('\\', "/"), path));
        }
    }

    fn asset_source_dir(repo_root: &Path, name: &str) -> PathBuf {
        let bundled = repo_root.join("bundle").join(name);
        if bundled.is_dir() {
            bundled
        } else {
            repo_root.join(name)
        }
    }

    fn collect_default_dir(dir: &Path, prefix: &str, files: &mut Vec<(String, PathBuf)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let relative = path
                .strip_prefix(dir)
                .ok()
                .map(|value| {
                    let child = value.to_string_lossy().replace('\\', "/");
                    if child.is_empty() {
                        prefix.to_string()
                    } else {
                        format!("{prefix}/{child}")
                    }
                })
                .unwrap_or_else(|| format!("{prefix}/{}", entry.file_name().to_string_lossy()));
            if path.is_dir() {
                if include_default_asset(&relative) {
                    collect_default_dir(&path, &relative, files);
                }
            } else if path.is_file() && include_default_asset(&relative) {
                files.push((relative, path));
            }
        }
    }

    fn include_default_asset(relative: &str) -> bool {
        let relative = relative.replace('\\', "/");
        if relative == "config.yaml" {
            return true;
        }
        if relative.starts_with("webroot/") {
            return true;
        }
        if relative.starts_with("managed/audio/")
            || relative.starts_with("managed/queues/")
            || relative.starts_with("managed/runtime/")
            || relative.starts_with("managed/services/")
            || relative == "managed/daemonSettings.json"
            || relative == "managed/goServiceRuntime.json"
            || relative == "managed/receiver_credentials.json"
        {
            return false;
        }
        if relative.starts_with("managed/") {
            return true;
        }
        if relative.starts_with("audio/alerts/")
            || relative.starts_with("audio/_previews/")
            || relative.starts_with("audio/_uploads/")
        {
            return false;
        }
        relative.starts_with("audio/static/")
            || relative.starts_with("audio/") && !relative["audio/".len()..].contains('/')
    }

    fn go_services() -> [(&'static str, &'static str); 8] {
        [
            ("haze-web", "./cmd/haze-web"),
            ("haze-data-ingest", "./cmd/haze-data-ingest"),
            ("haze-tts", "./cmd/haze-tts"),
            ("haze-product-render", "./cmd/haze-product-render"),
            ("haze-playlist", "./cmd/haze-playlist"),
            ("haze-webhook", "./cmd/haze-webhook"),
            ("haze-asr", "./cmd/haze-asr"),
            ("haze-ivr", "./cmd/haze-ivr"),
        ]
    }

    fn executable_name(base: &str) -> String {
        if cfg!(windows) {
            format!("{base}.exe")
        } else {
            base.to_string()
        }
    }

    struct GoOpusBuild {
        env: Vec<(&'static str, String)>,
        runtime_libraries: Vec<PathBuf>,
    }

    fn require_opus() -> bool {
        env::var("HAZE_ALLOW_NO_OPUS")
            .map(|value| value == "0" || value.eq_ignore_ascii_case("false"))
            .unwrap_or(true)
    }

    fn go_opus_build(_repo_root: &Path) -> Option<GoOpusBuild> {
        let mut envs = Vec::new();
        let mut libraries = Vec::new();
        if cfg!(windows) {
            let msys_root = env::var("MSYS2_ROOT").unwrap_or_else(|_| r"C:\msys64".to_string());
            let clang64_root = PathBuf::from(msys_root).join("clang64");
            let opus_bin = clang64_root.join("bin");
            let opus_lib = clang64_root.join("lib");
            if !opus_bin.join("x86_64-w64-mingw32-clang.exe").is_file()
                || !opus_bin.join("pkg-config.exe").is_file()
            {
                return None;
            }
            let path = env::var("PATH").unwrap_or_default();
            envs.push(("PATH", format!("{};{path}", opus_bin.display())));
            envs.push(("CGO_ENABLED", "1".to_string()));
            envs.push((
                "CC",
                opus_bin
                    .join("x86_64-w64-mingw32-clang.exe")
                    .display()
                    .to_string(),
            ));
            envs.push((
                "CXX",
                opus_bin
                    .join("x86_64-w64-mingw32-clang++.exe")
                    .display()
                    .to_string(),
            ));
            envs.push((
                "PKG_CONFIG",
                opus_bin.join("pkg-config.exe").display().to_string(),
            ));
            envs.push((
                "PKG_CONFIG_PATH",
                format!(
                    "{};{}",
                    opus_lib.join("pkgconfig").display(),
                    clang64_root.join("share").join("pkgconfig").display()
                ),
            ));
            for name in ["libopus-0.dll", "libopusfile-0.dll", "libogg-0.dll"] {
                libraries.push(opus_bin.join(name));
            }
            Some(GoOpusBuild {
                env: envs,
                runtime_libraries: libraries,
            })
        } else {
            let status = Command::new("pkg-config")
                .args(["--exists", "opus"])
                .status()
                .ok()?;
            if !status.success() {
                return None;
            }
            envs.push(("CGO_ENABLED", "1".to_string()));
            Some(GoOpusBuild {
                env: envs,
                runtime_libraries: libraries,
            })
        }
    }

    fn go_sherpa_runtime_libraries(go_root: &Path) -> Vec<PathBuf> {
        let target_os =
            env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| env::consts::OS.to_string());
        let target_arch =
            env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| env::consts::ARCH.to_string());
        let module = match target_os.as_str() {
            "windows" => "github.com/k2-fsa/sherpa-onnx-go-windows",
            "linux" => "github.com/k2-fsa/sherpa-onnx-go-linux",
            "macos" => "github.com/k2-fsa/sherpa-onnx-go-macos",
            _ => return Vec::new(),
        };
        let triple = match (target_os.as_str(), target_arch.as_str()) {
            ("windows", "x86_64") => "x86_64-pc-windows-gnu",
            ("windows", "x86") => "i686-pc-windows-gnu",
            ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
            ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
            ("linux", "arm") => "arm-unknown-linux-gnueabihf",
            ("macos", "x86_64") => "x86_64-apple-darwin",
            ("macos", "aarch64") => "aarch64-apple-darwin",
            _ => return Vec::new(),
        };
        let Some(module_dir) = go_module_dir(go_root, module) else {
            return Vec::new();
        };
        let lib_dir = module_dir.join("lib").join(triple);
        let Ok(entries) = std::fs::read_dir(lib_dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_file() && is_sherpa_runtime_library(path))
            .collect()
    }

    fn go_module_dir(go_root: &Path, module: &str) -> Option<PathBuf> {
        let output = Command::new("go")
            .current_dir(go_root)
            .args(["list", "-m", "-f", "{{.Dir}}", module])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            return None;
        }
        Some(PathBuf::from(path))
    }

    fn is_sherpa_runtime_library(path: &Path) -> bool {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        name.ends_with(".dll")
            || name.ends_with(".so")
            || name.contains(".so.")
            || name.ends_with(".dylib")
    }
}

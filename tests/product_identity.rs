use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn package_and_binary_use_sporos_identity() {
    let manifest =
        fs::read_to_string(repo_root().join("Cargo.toml")).expect("Cargo.toml should be readable");
    assert!(
        manifest
            .lines()
            .any(|line| line.trim() == "name = \"sporos\""),
        "Cargo package name must stay Sporos-native"
    );

    let binary_path = Path::new(env!("CARGO_BIN_EXE_sporos"));
    assert_eq!(
        Some(OsStr::new("sporos")),
        binary_path.file_stem(),
        "built binary name must stay Sporos-native"
    );
}

#[test]
fn runtime_visible_files_do_not_use_legacy_identity() {
    let root = repo_root();
    let mut violations = Vec::new();

    for path in scanned_files(&root) {
        let Ok(contents) = fs::read_to_string(&path) else {
            continue;
        };

        let normalized = contents.to_ascii_lowercase();
        for term in legacy_product_terms() {
            if normalized.contains(&term) {
                violations.push(format!(
                    "{} contains legacy product identity `{term}`",
                    path.strip_prefix(&root)
                        .expect("scanned path should be inside repo")
                        .display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "runtime-visible files must use Sporos identity:\n{}",
        violations.join("\n")
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn scanned_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = VecDeque::from([root.to_path_buf()]);
    let mut files = Vec::new();

    while let Some(path) = pending.pop_front() {
        let entries = fs::read_dir(&path).expect("repository directory should be readable");

        for entry in entries {
            let entry = entry.expect("repository entry should be readable");
            let path = entry.path();

            if should_skip(root, &path) {
                continue;
            }

            if path.is_dir() {
                pending.push_back(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }

    files
}

fn should_skip(root: &Path, path: &Path) -> bool {
    let relative_path = path
        .strip_prefix(root)
        .expect("scanned path should be inside repo");

    let mut components = relative_path
        .components()
        .filter_map(|component| component.as_os_str().to_str());

    matches!(components.next(), Some(".beads" | ".git" | "target"))
        || relative_path.starts_with("docs/internal")
        || relative_path == Path::new("tests/product_identity.rs")
}

fn legacy_product_terms() -> Vec<String> {
    [
        ["cross", "-seed"].concat(),
        ["cross", "seed"].concat(),
        ["cross", "_seed"].concat(),
    ]
    .into()
}

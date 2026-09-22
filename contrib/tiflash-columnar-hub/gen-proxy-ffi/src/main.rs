use std::{
    collections::hash_map::DefaultHasher,
    ffi::OsStr,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

const RAFT_STORE_PROXY_VERSION_PREFIX: &str = "RAFT_STORE_PROXY_VERSION";

fn collect_headers(dir: &Path, headers: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("Couldn't read FFI directory") {
        let path = entry.expect("Couldn't read FFI directory entry").path();
        if path.is_dir() {
            collect_headers(&path, headers);
        } else if path.extension() == Some(OsStr::new("h")) {
            headers.push(path);
        }
    }
}

fn ffi_version(ffi_dir: &Path) -> u64 {
    let mut headers = Vec::new();
    collect_headers(ffi_dir, &mut headers);
    headers.sort();

    let mut hasher = DefaultHasher::new();
    for header in headers {
        fs::read_to_string(header)
            .expect("Couldn't read FFI header")
            .hash(&mut hasher);
    }
    hasher.finish()
}

fn replace_version(path: &Path, replacement: &str) {
    let content = fs::read_to_string(path).expect("Couldn't read generated FFI bindings");
    assert_eq!(content.matches(RAFT_STORE_PROXY_VERSION_PREFIX).count(), 1);
    let declaration = format!("pub const {}: u64 = ", RAFT_STORE_PROXY_VERSION_PREFIX);
    let start = content
        .find(&declaration)
        .expect("Couldn't find RAFT_STORE_PROXY_VERSION");
    let end = content[start..]
        .find(';')
        .expect("Couldn't find RAFT_STORE_PROXY_VERSION terminator")
        + start
        + 1;
    let mut updated = content;
    updated.replace_range(start..end, replacement);
    fs::write(path, updated).expect("Couldn't write generated FFI bindings");
}

fn main() {
    let hub_dir = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let ffi_dir = hub_dir.join("hub-runtime/ffi/src/RaftStoreProxyFFI");
    let version = ffi_version(&ffi_dir);

    fs::write(
        ffi_dir.join("@version"),
        format!(
            "#pragma once\n#include <cstdint>\nnamespace DB {{ constexpr uint64_t {} = {}ull; }}",
            RAFT_STORE_PROXY_VERSION_PREFIX, version
        ),
    )
    .expect("Couldn't write FFI version header");
    // The Hub bindings include manually maintained declarations, so update only the ABI fingerprint.
    replace_version(
        &hub_dir.join("hub-runtime/src/interfaces.rs"),
        &format!(
            "pub const {}: u64 = {};",
            RAFT_STORE_PROXY_VERSION_PREFIX, version
        ),
    );
}

// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let workspace_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()),
    )
    .join("..");
    let lock_path = workspace_dir.join("Cargo.lock");
    let cloud_storage_engine_repo = workspace_dir.join("../cloud-storage-engine");

    println!("cargo:rerun-if-changed={}", lock_path.display());
    watch_git_head(&cloud_storage_engine_repo);

    // The workspace patches cloud-storage-engine crates to local paths, so the
    // lockfile may not carry a git source for `kvengine`. Prefer the local
    // submodule HEAD and keep the lockfile parser as a fallback.
    let hash = get_repo_git_hash(&cloud_storage_engine_repo)
        .or_else(|| {
            std::fs::read_to_string(&lock_path)
                .ok()
                .and_then(|content| extract_kvengine_git_hash(&content))
        })
        .unwrap_or_else(|| "Unknown".to_string());

    println!("cargo:rustc-env=CLOUD_STORAGE_ENGINE_GIT_HASH={hash}");
}

fn get_repo_git_hash(repo_path: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let hash = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if hash.is_empty() {
        None
    } else {
        Some(hash)
    }
}

fn watch_git_head(repo_path: &Path) {
    let dot_git_path = repo_path.join(".git");
    println!("cargo:rerun-if-changed={}", dot_git_path.display());

    let Some(git_dir) = resolve_git_dir(&dot_git_path) else {
        return;
    };

    let head_path = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head_path.display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display()
    );

    let Ok(head_content) = std::fs::read_to_string(&head_path) else {
        return;
    };
    let Some(head_ref) = head_content.strip_prefix("ref: ") else {
        return;
    };
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join(head_ref.trim()).display()
    );
}

fn resolve_git_dir(dot_git_path: &Path) -> Option<PathBuf> {
    if dot_git_path.is_dir() {
        return Some(dot_git_path.to_path_buf());
    }

    let git_dir = std::fs::read_to_string(dot_git_path).ok()?;
    let git_dir = git_dir.strip_prefix("gitdir: ")?.trim();
    let git_dir = dot_git_path.parent()?.join(git_dir);
    Some(git_dir)
}

fn extract_git_hash_from_source(source: &str) -> Option<String> {
    if let Some(hash_start) = source.rfind('#') {
        let hash = source[hash_start + 1..].trim_end_matches('"');
        if !hash.is_empty() {
            return Some(hash.to_string());
        }
    }

    if let Some(rev_start) = source.find("?rev=") {
        let rev_part = &source[rev_start + "?rev=".len()..];
        let hash = rev_part.split('#').next()?.trim_end_matches('"');
        if !hash.is_empty() {
            return Some(hash.to_string());
        }
    }

    None
}

fn extract_kvengine_git_hash(content: &str) -> Option<String> {
    for block in content.split("[[package]]") {
        if !block.contains("name = \"kvengine\"") {
            continue;
        }
        for line in block.lines() {
            let Some(source) = line.strip_prefix("source = \"") else {
                continue;
            };
            if let Some(hash) = extract_git_hash_from_source(source) {
                return Some(hash);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{extract_kvengine_git_hash, resolve_git_dir};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}_{}", name, suffix))
    }

    #[test]
    fn test_extract_kvengine_git_hash_from_rev() {
        let lock = r#"
[[package]]
name = "kvengine"
version = "0.0.1"
source = "git+https://github.com/tidbcloud/cloud-storage-engine.git?rev=a9d93252f2ad0cba95eec51a857cd867cd5e6567#a9d93252f2ad0cba95eec51a857cd867cd5e6567"
"#;
        assert_eq!(
            extract_kvengine_git_hash(lock),
            Some("a9d93252f2ad0cba95eec51a857cd867cd5e6567".to_string())
        );
    }

    #[test]
    fn test_extract_kvengine_git_hash_from_branch() {
        let lock = r#"
[[package]]
name = "kvengine"
version = "0.0.1"
source = "git+https://github.com/tidbcloud/cloud-storage-engine.git?branch=cloud-engine#a9d93252f2ad0cba95eec51a857cd867cd5e6567"
"#;
        assert_eq!(
            extract_kvengine_git_hash(lock),
            Some("a9d93252f2ad0cba95eec51a857cd867cd5e6567".to_string())
        );
    }

    #[test]
    fn test_resolve_git_dir_from_directory() {
        let tmp_dir = unique_temp_dir("hub_build_rs_git_dir");
        let git_dir = tmp_dir.join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();

        assert_eq!(resolve_git_dir(&git_dir), Some(git_dir));

        std::fs::remove_dir_all(tmp_dir).unwrap();
    }

    #[test]
    fn test_resolve_git_dir_from_file() {
        let tmp_dir = unique_temp_dir("hub_build_rs_git_file");
        let repo_dir = tmp_dir.join("repo");
        let actual_git_dir = tmp_dir.join("actual-git-dir");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&actual_git_dir).unwrap();
        std::fs::write(
            repo_dir.join(".git"),
            format!("gitdir: {}\n", actual_git_dir.display()),
        )
        .unwrap();

        assert_eq!(
            resolve_git_dir(&repo_dir.join(".git")),
            Some(actual_git_dir.clone())
        );

        std::fs::write(repo_dir.join(".git"), "gitdir: ../actual-git-dir\n").unwrap();
        assert_eq!(
            resolve_git_dir(&repo_dir.join(".git")),
            Some(repo_dir.join("../actual-git-dir"))
        );

        std::fs::remove_dir_all(tmp_dir).unwrap();
    }
}

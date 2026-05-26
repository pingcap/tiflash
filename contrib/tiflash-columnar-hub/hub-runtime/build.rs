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

use std::path::PathBuf;

fn main() {
    let lock_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock_path.display());

    let hash = std::fs::read_to_string(&lock_path)
        .ok()
        .and_then(|content| extract_kvengine_git_hash(&content))
        .unwrap_or_else(|| "Unknown".to_string());

    println!("cargo:rustc-env=CLOUD_STORAGE_ENGINE_GIT_HASH={hash}");
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
            if let Some(rev_start) = source.find("?rev=") {
                let rev_part = &source[rev_start + "?rev=".len()..];
                let hash = rev_part.split('#').next()?.trim_end_matches('"');
                return Some(hash.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::extract_kvengine_git_hash;

    #[test]
    fn test_extract_kvengine_git_hash() {
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
}

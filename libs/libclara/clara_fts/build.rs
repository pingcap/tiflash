// Copyright 2025 PingCAP, Inc.
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

use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/directory/merged/proto.proto"], &["src"])?;
    println!("cargo:rerun-if-changed=src/directory/merged/proto.proto");

    let files = [
        "src/tokenizer/mod.rs",
        "src/index_reader.rs",
        "src/index_writer.rs",
        "src/brute_searcher.rs",
    ];

    // Generate bridge only
    let _ = cxx_build::bridges(files);

    Ok(())
}

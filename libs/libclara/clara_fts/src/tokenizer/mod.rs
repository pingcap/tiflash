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

use lazy_static::lazy_static;

mod multilingual_v1;
mod standard_v1;

pub use multilingual_v1::*;
pub use standard_v1::*;

use tantivy::tokenizer::TokenizerManager;

lazy_static! {
    /// Shared for both indexed search and on-demand search.
    pub static ref TOKENIZERS: TokenizerManager = all_tokenizers();
}

fn all_tokenizers() -> TokenizerManager {
    let manager = TokenizerManager::new();
    manager.register("STANDARD_V1", standard_v1());
    manager.register("MULTILINGUAL_V1", multilingual_v1());
    manager
}

pub fn supports_tokenizer(tokenizer_name: &str) -> bool {
    TOKENIZERS.get(tokenizer_name).is_some()
}

#[cxx::bridge(namespace = "ClaraFTS")]
mod ffi {
    extern "Rust" {
        fn supports_tokenizer(tokenizer_name: &str) -> bool;
    }
}

#[cfg(test)]
pub mod tests {
    use tantivy_tokenizer_api::TokenStream;

    /// A helper function to debug tokenize API in tests. It turns "世界你好" into "世界/你好".
    pub trait DebugTokenize {
        fn debug_tokenize(&mut self, text: &str) -> String;
    }

    impl DebugTokenize for tantivy::tokenizer::TextAnalyzer {
        fn debug_tokenize(&mut self, text: &str) -> String {
            let mut token_stream = self.token_stream(text);
            let mut tokens = Vec::new();
            token_stream.process(&mut |token: &tantivy_tokenizer_api::Token| {
                tokens.push(token.text.clone());
            });
            tokens.join("/")
        }
    }
}

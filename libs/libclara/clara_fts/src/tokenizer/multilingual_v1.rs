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

use std::sync::Arc;

use tantivy::tokenizer::TextAnalyzer;

pub fn multilingual_v1() -> TextAnalyzer {
    MultiLingualTokenizer::new().into()
}

#[derive(Clone)]
pub struct MultiLingualTokenizer {
    tokenizer: Arc<charabia::Tokenizer<'static>>,
}

impl MultiLingualTokenizer {
    pub fn new() -> Self {
        let tokenizer = charabia::TokenizerBuilder::default().into_tokenizer();
        Self {
            tokenizer: Arc::new(tokenizer),
        }
    }
}

impl Default for MultiLingualTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

impl tantivy_tokenizer_api::Tokenizer for MultiLingualTokenizer {
    type TokenStream<'a> = MultiLingualTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let tokens_iter = self.tokenizer.tokenize(text);
        MultiLingualTokenStream {
            iter: tokens_iter,
            token: tantivy_tokenizer_api::Token::default(),
        }
    }
}

pub struct MultiLingualTokenStream<'a> {
    iter: charabia::normalizer::NormalizedTokenIter<'a, 'a, 'a, 'a>,
    token: tantivy_tokenizer_api::Token,
}

impl<'a> tantivy_tokenizer_api::TokenStream for MultiLingualTokenStream<'a> {
    /// Advance to the next token
    ///
    /// Returns false if there are no other tokens.
    fn advance(&mut self) -> bool {
        loop {
            let raw_token = self.iter.next();
            if raw_token.is_none() {
                // No more tokens
                self.token.reset();
                return false;
            }

            let raw_token = raw_token.unwrap();
            if !raw_token.is_word() {
                // If not a word, just skip and try next.
                continue;
            }

            self.token.offset_from = raw_token.byte_start;
            self.token.offset_to = raw_token.byte_end;
            self.token.position = self.token.position.wrapping_add(1);
            self.token.text = raw_token.lemma.into_owned();
            return true;
        }
    }

    /// Returns a reference to the current token.
    #[inline(always)]
    fn token(&self) -> &tantivy_tokenizer_api::Token {
        &self.token
    }

    /// Returns a mutable reference to the current token.
    #[inline(always)]
    fn token_mut(&mut self) -> &mut tantivy_tokenizer_api::Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tests::DebugTokenize;

    #[test]
    fn test_tokenizer() {
        let mut t = multilingual_v1();
        assert_eq!(
            t.debug_tokenize("今年前两月社会物流总额同比增5.3%"),
            "今年/前/两/月/社会/物流/总额/同比/增/5/3",
        );
        assert_eq!(
            t.debug_tokenize("中国—东盟合作树立“全球南方”联合自强重要范本（国际论坛）"),
            "中国/东盟/合作/树立/全球/南方/联合/自强/重要/范本/国际/论坛",
        );
        assert_eq!(
            t.debug_tokenize("· 加强文明对话 促进理解信任（聚焦博鳌亚洲论坛2025年年会）"),
            "加强/文明/对话/促进/理解/信任/聚焦/博鳌/亚洲/论坛/2025/年/年/会"
        );
        assert_eq!(
            t.debug_tokenize("The quick (\"brown\") fox can't jump 32.3 feet, right?"),
            "the/quick/brown/fox/can/t/jump/32/3/feet/right",
        );
        assert_eq!(
            t.debug_tokenize("ミャンマー大地震 死者2700人超 軍トップ“3000人超に増える”"),
            "みゃんまあ/大/地震/死者/2700/人/超/軍/とっふ\u{309a}/3/0/0/0/人/超/に/増える",
        );
    }
}

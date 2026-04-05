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

use tantivy::tokenizer::*;

pub fn standard_v1() -> TextAnalyzer {
    // The same as Tantivy's DEFAULT tokenizer
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::tests::DebugTokenize;

    #[test]
    fn test_tokenizer() {
        let mut t = standard_v1();
        assert_eq!(
            t.debug_tokenize("Trump says ‘there are methods’ for seeking a third term, adding that he’s ‘not joking’"),
            "trump/says/there/are/methods/for/seeking/a/third/term/adding/that/he/s/not/joking",
        );
        assert_eq!(
            t.debug_tokenize("The quick (\"brown\") fox can't jump 32.3 feet, right?"),
            "the/quick/brown/fox/can/t/jump/32/3/feet/right",
        );

        // This example shows the standard tokenizer behaves badly on CJK text.
        assert_eq!(
            t.debug_tokenize("· 加强文明对话 促进理解信任（聚焦博鳌亚洲论坛2025年年会）"),
            "加强文明对话/促进理解信任/聚焦博鳌亚洲论坛2025年年会"
        );
        assert_eq!(t.debug_tokenize("今年前两月社会物流总额同比增5.3%"), "3",); // Strip due to long filter
        assert_eq!(
            t.debug_tokenize("ミャンマー大地震 死者2700人超 軍トップ“3000人超に増える”"),
            "ミャンマー大地震/死者2700人超/軍トップ/3000人超に増える",
        );
    }
}

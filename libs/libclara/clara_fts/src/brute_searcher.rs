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

use std::collections::HashMap;

use anyhow::{bail, Result};
use tantivy::tokenizer::TextAnalyzer;

/// Search for a specified query within a dataset without any index.
/// It is faster than building an index over this dataset on the fly and then search the index.
/// However it will be magnitudes slower than a pre-built index when the dataset does not change.
///
/// This searcher use the same BM25 algorithm as Tantivy, aims to produce exactly the
/// same score for the same query and source text.
pub struct BruteScoredSearcher {
    /// Used for both source text and query text.
    /// TODO: Support using different tokenizers for source text and query text.
    text_tokenizer: TextAnalyzer,

    /// Pre-tokenized query.
    query_tokens: HashMap<String, /* token_index */ usize>,

    // Fields below are for BM25 statistics.
    // Tantivy is calculating BM25 by summing token scores and if there are duplicate tokens in the query,
    // they will be counted multiple times. We provide the same score as Tantivy.
    //
    /// The number of occurs for each token in the query: `[Token0, Token1, ...]`
    /// if there are duplicate token in one query they will be counted here.
    token_occurs: Vec<u32>,

    /// Number of tokens each row: `[Row0, Row1, ...]`
    /// Here we do not store the actual tokens number, but store "fieldnorm id" (which maps u32 length to u8 id).
    /// This is not for saving memory, but to produce the same score result as Tantivy.
    per_row_n_tokens: Vec<u8>,
    /// Although per-row token number is recorded as fieldnorm id, the total tokens are not treated in this way.
    /// This is to produce the same score result as Tantivy.
    total_tokens: u32,
    /// Number of hits for each token for each row: `[Row0Token0, Row0Token1, ... Row1Token0, Row1Token1, ...]`
    per_row_per_token_hits: Vec<u32>,
    /// Number of hit docs for each token: `[Token0, Token1, ...]`
    per_token_doc_hits: Vec<u32>,

    /// A local buffer reused in each `add_document()`.
    buf_per_token_hits: Vec<u32>,
}

impl BruteScoredSearcher {
    pub fn new(tokenizer_name: &str, query: &str) -> Result<Self> {
        use std::collections::hash_map::Entry;

        let tokenizers = crate::tokenizer::TOKENIZERS.clone();
        let tokenizer = tokenizers.get(tokenizer_name);
        if tokenizer.is_none() {
            bail!("Tokenizer {:?} not found", tokenizer_name);
        }

        let mut token_occurs = Vec::new();
        let mut token_idx = 0;
        let mut query_tokenizer = tokenizer.clone().unwrap();
        let mut query_tokens = query_tokenizer.token_stream(query);
        let mut ret_query_tokens = HashMap::new();
        while query_tokens.advance() {
            let token = query_tokens.token_mut();
            let text = std::mem::take(&mut token.text);
            let entry = ret_query_tokens.entry(text);
            match entry {
                Entry::Occupied(e) => {
                    token_occurs[*e.get()] += 1;
                }
                Entry::Vacant(e) => {
                    e.insert(token_idx);
                    token_idx += 1;
                    token_occurs.push(1);
                }
            }
        }

        assert_eq!(token_occurs.len(), ret_query_tokens.len());
        let uniq_tokens_n = ret_query_tokens.len();

        Ok(Self {
            text_tokenizer: tokenizer.unwrap(),
            query_tokens: ret_query_tokens,
            token_occurs,
            per_row_n_tokens: Vec::new(),
            total_tokens: 0,
            per_row_per_token_hits: Vec::new(),
            per_token_doc_hits: vec![0; uniq_tokens_n],
            buf_per_token_hits: Vec::new(),
        })
    }

    #[inline]
    pub fn total_num_docs(&self) -> usize {
        self.per_row_n_tokens.len()
    }

    /// Reserves capacity for at least `additional` more documents (includes null) to be inserted.
    pub fn reserve(&mut self, additional: usize) {
        self.per_row_n_tokens.reserve(additional);
        self.per_row_per_token_hits
            .reserve(additional * self.query_tokens.len());
    }

    pub fn clear(&mut self) {
        self.per_row_n_tokens.clear();
        self.total_tokens = 0;
        self.per_row_per_token_hits.clear();
        self.per_token_doc_hits.fill(0);
    }

    pub fn add_document(&mut self, body: &str) {
        // We don't actually store the tokenlized data, only store metadata,
        // which is enough.

        self.buf_per_token_hits.clear();
        self.buf_per_token_hits.resize(self.query_tokens.len(), 0);

        let mut this_doc_tokens = 0u32;
        let mut src_tokens = self.text_tokenizer.token_stream(body);
        while src_tokens.advance() {
            let token = src_tokens.token();
            this_doc_tokens += 1;
            let token_idx_ = self.query_tokens.get(&token.text);
            if let Some(token_idx) = token_idx_ {
                // Token occurs in the query.
                self.buf_per_token_hits[*token_idx] += 1;
            }
        }

        self.per_row_n_tokens
            .push(crate::tantivy_compat::fieldnorm_code::fieldnorm_to_id(
                this_doc_tokens,
            ));
        self.total_tokens += this_doc_tokens;
        self.per_row_per_token_hits.extend(&self.buf_per_token_hits);
        for idx in 0..self.buf_per_token_hits.len() {
            if self.buf_per_token_hits[idx] > 0 {
                // Even if a token is hit multiple times we only count it once.
                self.per_token_doc_hits[idx] += 1;
            }
        }
    }

    pub fn add_null(&mut self) {
        self.per_row_n_tokens.push(0);
        self.per_row_per_token_hits.resize(
            self.per_row_per_token_hits.len() + self.query_tokens.len(),
            0,
        );
    }

    /// The result is always sorted by RowID (but may miss some rows if row is null or not hit).
    ///
    /// This function contains heavy computation and the result won't change if there is no new doc.
    #[allow(clippy::needless_range_loop)]
    pub fn search(
        &self,
        filter: &crate::BitmapFilter,
        results: &mut Vec<super::ScoredResult>,
    ) -> Result<()> {
        results.clear();

        let total_num_docs = self.per_row_n_tokens.len();
        let uniq_tokens = self.query_tokens.len();

        if total_num_docs == 0 {
            return Ok(());
        }

        if !filter.match_all && total_num_docs != filter.match_partial.len() {
            bail!(
                "Invalid bitmap filter, expected length {}, got {}",
                total_num_docs,
                filter.match_partial.len()
            );
        }

        let mut weights_per_token = Vec::with_capacity(uniq_tokens);
        {
            let avg_fieldnorm = self.total_tokens as f32 / total_num_docs as f32;
            for i in 0..uniq_tokens {
                let term_doc_freq = self.per_token_doc_hits[i];
                let weight = tantivy::query::Bm25Weight::for_one_term(
                    term_doc_freq as u64,
                    total_num_docs as u64,
                    avg_fieldnorm,
                );
                weights_per_token.push(weight);
            }
        }

        results.reserve(total_num_docs);
        for doc_id in 0..total_num_docs {
            if !filter.match_all && filter.match_partial[doc_id] == 0 {
                continue;
            }
            let doc_id_mul_tokens = doc_id * uniq_tokens;
            let mut score = 0.0;
            for token_idx in 0..uniq_tokens {
                let term_freq = self.per_row_per_token_hits[doc_id_mul_tokens + token_idx];
                if term_freq == 0 {
                    continue;
                }
                let token_score = weights_per_token[token_idx].score(
                    self.per_row_n_tokens[doc_id], //
                    term_freq,
                );
                score += token_score * (self.token_occurs[token_idx] as f32);
            }
            if score > 0.0 {
                results.push(super::ScoredResult {
                    doc_id: doc_id as u32,
                    score,
                });
            }
        }

        Ok(())
    }
}

/// For FFI
fn new_brute_scored_searcher(
    tokenizer_name: &str,
    query: &str,
) -> Result<Box<BruteScoredSearcher>> {
    let searcher = BruteScoredSearcher::new(tokenizer_name, query)?;
    Ok(Box::new(searcher))
}

#[cxx::bridge(namespace = "ClaraFTS")]
mod ffi {
    extern "C++" {
        include!("clara_fts/src/index_reader.rs.h");

        type ScoredResult = crate::ScoredResult;

        type BitmapFilter<'a> = crate::BitmapFilter<'a>;
    }

    extern "Rust" {
        type BruteScoredSearcher;

        fn new_brute_scored_searcher(
            tokenizer_name: &str,
            query: &str,
        ) -> Result<Box<BruteScoredSearcher>>;

        fn add_document(self: &mut BruteScoredSearcher, body: &str);

        fn add_null(self: &mut BruteScoredSearcher);

        fn total_num_docs(self: &BruteScoredSearcher) -> usize;

        fn reserve(self: &mut BruteScoredSearcher, additional: usize);

        fn clear(self: &mut BruteScoredSearcher);

        fn search(
            self: &BruteScoredSearcher,
            filter: &BitmapFilter,
            results: &mut Vec<ScoredResult>,
        ) -> Result<()>;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn is_index_match(src: &str, query: &str) -> Result<bool> {
        let mut index_writer = crate::IndexWriterInMemory::new("STANDARD_V1")?;
        index_writer.add_document(src)?;
        let buffer = index_writer.finalize()?;
        let index_reader = crate::IndexReader::new_memory(buffer)?;
        let mut results = Vec::new();
        index_reader.search_no_score(query, &crate::BitmapFilter::all_match(), &mut results)?;
        Ok(!results.is_empty())
    }

    fn assert_match_eq(src: &str, query: &str) -> Result<()> {
        let index_match_result = is_index_match(src, query)?;
        let mut searcher = BruteScoredSearcher::new("STANDARD_V1", query)?;
        searcher.add_document(src);
        let mut results = Vec::new();
        searcher.search(&crate::BitmapFilter::all_match(), &mut results)?;
        let noindex_match_result = !results.is_empty();
        assert_eq!(
            index_match_result, noindex_match_result,
            "Search `{}` in `{}` got different result: found_by_index={}, found_by_noindex={}",
            query, src, index_match_result, noindex_match_result
        );
        Ok(())
    }

    #[test]
    fn test_search_same_as_index() -> Result<()> {
        // This test verifies that the BruteScoredSearcher returns the same result
        // as using the index.

        let src = "Being too popular can be such a hassle";
        let queries = vec![
            "popular",
            "people",
            "peoples",
            "can",
            "being",
            "beING",
            "CAN",
            "furina",
            "eing",
            "foo bar",
            "foo bar be",
            "ass",
            "ass a",
            "   ass    a   ",
            "elssah",
        ];
        for query in queries {
            assert_match_eq(src, query)?;
        }
        Ok(())
    }

    const SCORE_SAMPLE_DOCS: &[&str] = &[
        /* 0 */ "machine learning machine learning machine learning",
        /* 1 */ "machine learning algorithms. Machine learning models. Machine learning optimization.",
        /* 2 */ "Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning (Machine learning (Machine learning (Machine learning (Machine learning (is used in AI. Machine learning ends.",
        /* 3 */ "Understanding machine learning: basics of learning from machines. Machine learning requires data.",
        /* 4 */ "machine learning",
        /* 5 */ "This 50-word document briefly mentions machine learning once. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. Generic text about AI. ",
        /* 6 */ "Learning machine learning machine. Learning machine applications.",
        /* 7 */ "Space Tourism: Risks and Opportunities - Private companies like SpaceX and Blue Origin are making suborbital flights accessible, but safety protocols remain a critical concern.",
        /* 8 */ "Sustainable Fashion Trends - Eco-friendly materials like mushroom leather and recycled polyester are reshaping the fashion industry’s environmental impact.",
        /* 9 */ "AI-powered tools are transforming medical imaging analysis. Deep learning algorithms now detect early-stage tumors with 95% accuracy."
        // TODO: Add lines with > 100 words
    ];

    fn assert_scores_eq(query: &str) -> Result<()> {
        let mut index_writer = crate::IndexWriterInMemory::new("STANDARD_V1")?;
        for doc in SCORE_SAMPLE_DOCS {
            index_writer.add_document(doc)?;
        }
        let buffer = index_writer.finalize()?;
        let index_reader = crate::IndexReader::new_memory(buffer)?;
        let mut results_index = Vec::new();
        index_reader.search_scored(query, &crate::BitmapFilter::all_match(), &mut results_index)?;

        let mut searcher = BruteScoredSearcher::new("STANDARD_V1", query)?;
        for doc in SCORE_SAMPLE_DOCS {
            searcher.add_document(doc);
        }
        let mut results_noindex = Vec::new();
        searcher.search(&crate::BitmapFilter::all_match(), &mut results_noindex)?;

        assert_eq!(
            results_index.len(),
            results_noindex.len(),
            "Length mismatch: index={:?}, noindex={:?}",
            results_index,
            results_noindex
        );
        for (index_result, noindex_result) in results_index.iter().zip(results_noindex.iter()) {
            assert_eq!(
                index_result.doc_id, noindex_result.doc_id,
                "Docid mismatch: index={:?}, noindex={:?}",
                results_index, results_noindex
            );
            assert!(
                (index_result.score - noindex_result.score).abs() < 0.001,
                "Score mismatch: index={:?}, noindex={:?}",
                results_index,
                results_noindex
            );
        }
        Ok(())
    }

    #[test]
    fn test_search_score_same_as_index() -> Result<()> {
        // This test verifies that the BruteScoredSearcher returns the same result
        // as using the index.
        let queries = vec![
            "machine learn",
            "machine learning",
            "machine learning machine learning",
            "machine learning machine",
            "machine learning learning",
            "machine learning algorithms",
            "machine learning optimization",
            "machine learning is used in AI",
            "machine learning ends",
            "machine learning requires data",
            "learning machine",
            "learning machine applications",
            "space tourism",
            "tantivy bm25",
        ];
        for query in queries {
            assert_scores_eq(query)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod benches {
    use tantivy::directory::DirectoryClone;

    use super::*;
    use std::hint::black_box;
    use std::{fs, io};

    fn prepare_bench_data() -> Vec<String> {
        use io::BufRead;
        // > The working directory of every benchmark is set to the
        // > root directory of the package the benchmark belongs to.
        let p = std::path::Path::new("../testdata/Arts_Crafts_and_Sewing.jsonl.gz");
        if !p.exists() {
            panic!("Test data file does not exist, see testdata/README.md for details");
        }
        let gz_file = flate2::read::GzDecoder::new(fs::File::open(p).unwrap());
        let buf_gz_file = io::BufReader::new(gz_file);
        let lines = buf_gz_file.lines();
        let mut data = Vec::new();
        for line in lines.map_while(Result::ok) {
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            data.push(v["text"].as_str().unwrap().to_string());
            if data.len() >= 100 {
                break;
            }
        }

        data
    }

    #[bench]
    #[ignore]
    fn bench_scored_brute_search(b: &mut test::Bencher) {
        let data = prepare_bench_data();

        let mut results = Vec::new();
        b.iter(|| {
            let mut s = BruteScoredSearcher::new("STANDARD_V1", "sewing machine").unwrap();
            for d in &data {
                s.add_document(d);
            }
            s.search(&crate::BitmapFilter::all_match(), &mut results)
                .unwrap();
            let matches = results.len();
            black_box(matches);
            assert_eq!(4, matches);
        });
    }

    #[bench]
    #[ignore]
    fn bench_scored_tantivy_searcher_prebuilt(b: &mut test::Bencher) {
        let data = prepare_bench_data();

        let mut idx_writer = crate::index_writer::TantivyIndexWriter::new(
            "STANDARD_V1",
            tantivy::directory::RamDirectory::create().box_clone(),
        )
        .unwrap();
        for d in &data {
            idx_writer.add_document(d).unwrap();
        }
        let idx = idx_writer.finalize().unwrap();
        let idx_reader = crate::IndexReader::from_tantivy_index(idx).unwrap();

        let mut results = Vec::new();
        b.iter(|| {
            idx_reader
                .search_scored(
                    "sewing machine",
                    &crate::BitmapFilter::all_match(),
                    &mut results,
                )
                .unwrap();
            let matches = results.len();
            black_box(matches);
            assert_eq!(4, matches);
        });
    }

    #[bench]
    #[ignore]
    fn bench_scored_tantivy_searcher_on_demand(b: &mut test::Bencher) {
        let data = prepare_bench_data();

        let mut results = Vec::new();
        b.iter(|| {
            let mut idx_writer = crate::index_writer::TantivyIndexWriter::new(
                "STANDARD_V1",
                tantivy::directory::RamDirectory::create().box_clone(),
            )
            .unwrap();
            for d in &data {
                idx_writer.add_document(d).unwrap();
            }
            let idx = idx_writer.finalize().unwrap();
            let idx_reader = crate::IndexReader::from_tantivy_index(idx).unwrap();
            idx_reader
                .search_scored(
                    "sewing machine",
                    &crate::BitmapFilter::all_match(),
                    &mut results,
                )
                .unwrap();
            let matches = results.len();
            black_box(matches);
            assert_eq!(4, matches);
        });
    }
}

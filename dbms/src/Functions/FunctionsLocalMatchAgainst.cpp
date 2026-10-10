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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/UTF8Helpers.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsLocalMatchAgainst.h>
#include <Functions/LocalMatchAgainstTokenChars.h>
#include <Poco/UTF8String.h>
#include <TiDB/Collation/Collator.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

namespace
{
constexpr size_t default_min_token_size = 3;
constexpr size_t default_max_token_size = 84;
constexpr size_t default_ngram_token_size = 2;
// This is the Local MATCH semantic protocol version, not the TiDB/TiFlash
// product version. Version 2 fixes the Boolean AST interpretation, analyzer
// behavior, and built-in stopword set. TiFlash must support a version before
// TiDB emits it; unknown versions are rejected, and existing versions must
// never be reinterpreted with changed semantics.
// Version 2 returns zero for NULL searches and uses MySQL's multibyte NGRAM
// document scanner. STANDARD and the NGRAM Boolean lexer use BMP word
// characters; filtered phrases also verify the unfiltered document stream.
constexpr UInt32 local_match_against_protocol_version = 2;

struct FullTextToken
{
    // Query terms and the no-collator lowercase path own their spelling.
    // Document views refer only to the current input column, which outlives
    // both analyzed streams and matching. Never retain them across blocks.
    String text;
    std::string_view source;
    size_t position = 0;
    size_t code_points = 0;

    std::string_view value() const { return source.data() ? source : std::string_view(text); }
};

class FullTextColumn
{
public:
    using Container = std::vector<FullTextToken>;
    using iterator = Container::iterator;
    using const_iterator = Container::const_iterator;

    void clear() { used = 0; }

    void reserve(size_t size) { tokens.reserve(size); }

    void append(std::string_view text, size_t position, size_t code_points, bool borrow = false)
    {
        if (used == tokens.size())
            tokens.emplace_back();
        auto & token = tokens[used++];
        if (borrow)
            token.source = text;
        else
        {
            token.source = {};
            token.text.assign(text.data(), text.size());
        }
        token.position = position;
        token.code_points = code_points;
    }

    template <typename Predicate>
    void filterInPlace(Predicate predicate)
    {
        size_t write = 0;
        for (size_t read = 0; read < used; ++read)
        {
            if (!predicate(tokens[read]))
                continue;
            if (write != read)
                std::swap(tokens[write], tokens[read]);
            ++write;
        }
        used = write;
    }

    bool empty() const { return used == 0; }
    size_t size() const { return used; }
    FullTextToken & front() { return tokens.front(); }
    const FullTextToken & front() const { return tokens.front(); }
    FullTextToken & operator[](size_t index) { return tokens[index]; }
    const FullTextToken & operator[](size_t index) const { return tokens[index]; }
    iterator begin() { return tokens.begin(); }
    iterator end() { return tokens.begin() + used; }
    const_iterator begin() const { return tokens.begin(); }
    const_iterator end() const { return tokens.begin() + used; }

private:
    Container tokens;
    size_t used = 0;
};

struct BooleanClause
{
    enum class Modifier
    {
        Should,
        Must,
        MustNot,
    };

    Modifier modifier = Modifier::Should;
    bool phrase = false;
    bool prefix = false;
    std::vector<String> terms;
    std::vector<size_t> offsets;
    std::unique_ptr<BooleanClause> verification;
    std::vector<std::unique_ptr<TiDB::ITiDBCollator::IPattern>> prefix_matchers;
};

struct AnalyzerScratch
{
    std::vector<size_t> char_boundaries;
    String lowercase_run;
};

struct CompiledBooleanQuery
{
    std::vector<BooleanClause> clauses;
    size_t must_clause_count = 0;
    bool has_must_not = false;
    bool matches_nothing = false;
    bool needs_verification = false;
};

void lowercaseTokenInPlace(String & token)
{
    const bool is_ascii = std::all_of(token.begin(), token.end(), [](unsigned char c) { return c < 0x80; });
    if (is_ascii)
    {
        for (char & c : token)
            if (c >= 'A' && c <= 'Z')
                c = static_cast<char>(c + ('a' - 'A'));
        return;
    }
    Poco::UTF8::toLowerInPlace(token);
}

bool isFullTextToken(UInt32 code_point)
{
    return code_point <= 0xFFFF && LocalMatchAgainst::isTokenChar(code_point);
}

std::pair<UInt32, size_t> decodeCodePoint(std::string_view text, size_t offset)
{
    // Most document bytes are ASCII. Keep the multibyte/invalid-byte
    // semantics below, but avoid calling the generic decoder for ASCII.
    const auto byte = static_cast<unsigned char>(text[offset]);
    if (byte < 0x80)
        return {byte, 1};
    const auto decoded = UTF8::utf8Decode(text.data() + offset, text.size() - offset);
    if (decoded.second == 0 || decoded.first == UTF8::UTF8_Error || decoded.second > text.size() - offset)
        return {0xFFFD, 1}; // Go's utf8.DecodeRuneInString also consumes one invalid byte as RuneError.
    return {decoded.first, decoded.second};
}

template <typename Consumer>
void scanStandardTokens(std::string_view text, Consumer && consume)
{
    size_t position = 0;
    for (size_t i = 0; i < text.size();)
    {
        const auto [code_point, length] = decodeCodePoint(text, i);
        if (!isFullTextToken(code_point))
        {
            i += length;
            continue;
        }

        const size_t token_start = i;
        // The first character has already been decoded and classified.
        size_t code_points = 1;
        i += length;
        while (i < text.size())
        {
            const auto [code_point, length] = decodeCodePoint(text, i);
            if (!isFullTextToken(code_point))
                break;
            ++code_points;
            i += length;
        }

        // Positions include source tokens subsequently removed by filtering.
        consume(text.substr(token_start, i - token_start), position++, code_points);
    }
}

void tokenizeTextInto(std::string_view text, FullTextColumn & result, const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    result.clear();
    scanStandardTokens(text, [&](std::string_view token, size_t position, size_t code_points) {
        result.append(token, position, code_points);
        if (!collator)
            lowercaseTokenInPlace(result[result.size() - 1].text);
    });
}

FullTextColumn tokenizeText(std::string_view text, const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    FullTextColumn result;
    tokenizeTextInto(text, result, collator);
    return result;
}

const std::unordered_set<String> & defaultStopwords()
{
    static const std::unordered_set<String> stopwords{
        "a",    "about", "an",  "are",  "as",   "at",    "be",  "by",   "com",  "de",  "en",   "for",
        "from", "how",   "i",   "in",   "is",   "it",    "la",  "of",   "on",   "or",  "that", "the",
        "this", "to",    "was", "what", "when", "where", "who", "will", "with", "und", "www"};
    return stopwords;
}

// Owned collation keys are compiled once per executeImpl, then reused by
// query analysis and every document in the block. The scratch key is local
// to that execution, never shared across concurrent calls. Use the stopword
// (server) collation, not the MATCH column collation.
class BuiltinStopwordLookup
{
public:
    explicit BuiltinStopwordLookup(TiDB::TiDBCollatorPtr collator_)
        : collator(collator_)
    {
        for (const auto & stopword : defaultStopwords())
        {
            const auto key = collator->sortKeyFastPath(stopword.data(), stopword.size(), key_buffer);
            // sortKeyFastPath may reference its input or key_buffer; copy it.
            String owned_key(key.data, key.size);
            keys.insert(owned_key);
            // The built-in list is ASCII. Preserve NGRAM's code-point length
            // buckets even for collations with expansions/ignorable weights.
            keys_by_length[stopword.size()].insert(std::move(owned_key));
        }
    }

    bool contains(std::string_view token) const { return contains(token, keys); }

    bool contains(std::string_view token, size_t code_points) const
    {
        const auto bucket = keys_by_length.find(code_points);
        return bucket != keys_by_length.end() && contains(token, bucket->second);
    }

private:
    struct KeyHash
    {
        using is_transparent = void;
        size_t operator()(std::string_view key) const { return std::hash<std::string_view>{}(key); }
    };
    using Keys = std::unordered_set<String, KeyHash, std::equal_to<>>;

    bool contains(std::string_view token, const Keys & lookup) const
    {
        // sortKey (rather than sortKeyNoTrim) preserves PAD SPACE/NO PAD
        // equality, as well as the collator's case/accent semantics.
        const auto key = collator->sortKeyFastPath(token.data(), token.size(), key_buffer);
        return lookup.contains(std::string_view(key.data, key.size));
    }

    TiDB::TiDBCollatorPtr collator;
    Keys keys;
    std::unordered_map<size_t, Keys> keys_by_length;
    mutable String key_buffer;
};

bool isDefaultStopword(std::string_view token, const BuiltinStopwordLookup * stopwords)
{
    return stopwords ? stopwords->contains(token) : defaultStopwords().contains(Poco::UTF8::toLower(String(token)));
}

bool containsNgramStopword(
    std::string_view token,
    const std::vector<size_t> & char_boundaries,
    size_t ngram_start,
    size_t ngram_token_size,
    const BuiltinStopwordLookup * stopwords)
{
    const size_t ngram_end = ngram_start + ngram_token_size;
    for (size_t start = ngram_start; start < ngram_end; ++start)
    {
        for (size_t end = start + 1; end <= ngram_end; ++end)
        {
            const auto candidate = token.substr(char_boundaries[start], char_boundaries[end] - char_boundaries[start]);
            if (!stopwords && defaultStopwords().contains(Poco::UTF8::toLower(String(candidate))))
                return true;
            if (stopwords && stopwords->contains(candidate, end - start))
                return true;
        }
    }
    return false;
}

void analyzeTextInto(
    std::string_view text,
    FullTextColumn & result,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true,
    bool borrow_document = false)
{
    result.clear();
    scanStandardTokens(text, [&](std::string_view token, size_t position, size_t code_points) {
        // Filter before materializing. Stopwords use the source spelling and
        // server collation, independently of the MATCH column collation.
        if (code_points < min_token_size || code_points > max_token_size
            || (enable_stopword && isDefaultStopword(token, stopwords)))
            return;
        result.append(token, position, code_points, borrow_document && collator);
        if (!collator)
            lowercaseTokenInPlace(result[result.size() - 1].text);
    });
}

FullTextColumn analyzeText(
    std::string_view text,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    FullTextColumn result;
    analyzeTextInto(text, result, collator, stopwords, min_token_size, max_token_size, enable_stopword);
    return result;
}

void analyzeNgramTextInto(
    std::string_view text,
    FullTextColumn & result,
    AnalyzerScratch & scratch,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr,
    bool enable_stopword = true,
    bool borrow_document = false)
{
    result.clear();
    if (ngram_token_size == 0)
        return;

    const bool preserve_case = enable_stopword && stopwords;
    size_t next_position_base = 0;
    auto & char_boundaries = scratch.char_boundaries;
    for (size_t offset = 0; offset < text.size();)
    {
        const auto [first_code_point, first_length] = decodeCodePoint(text, offset);
        // Stop the document on malformed UTF-8, retain all valid multibyte
        // characters, and split runs on ASCII nonwords, as MySQL does.
        if (first_length == 1 && first_code_point >= 0x80)
            break;
        if (first_length == 1 && !isFullTextToken(first_code_point))
        {
            offset += first_length;
            continue;
        }
        const size_t run_start = offset;
        char_boundaries.clear();
        char_boundaries.push_back(0);
        offset += first_length;
        char_boundaries.push_back(offset - run_start);
        while (offset < text.size())
        {
            const auto [code_point, length] = decodeCodePoint(text, offset);
            if (length == 1 && (code_point >= 0x80 || !isFullTextToken(code_point)))
                break;
            offset += length;
            char_boundaries.push_back(offset - run_start);
        }

        std::string_view run = text.substr(run_start, offset - run_start);
        const size_t char_count = char_boundaries.size() - 1;
        const size_t base_position = next_position_base;
        if (char_count < ngram_token_size)
        {
            // A short document run emits no grams and contributes no position.
            continue;
        }

        if (!collator && !preserve_case)
        {
            // The no-collator path lowercases a whole run before splitting.
            // Unicode case mapping can change byte widths, so only that path
            // needs a copy and another boundary scan. Never borrow scratch.
            scratch.lowercase_run.assign(run.data(), run.size());
            lowercaseTokenInPlace(scratch.lowercase_run);
            run = scratch.lowercase_run;
            char_boundaries.clear();
            char_boundaries.push_back(0);
            for (size_t i = 0; i < run.size();)
            {
                i += decodeCodePoint(run, i).second;
                char_boundaries.push_back(i);
            }
        }

        for (size_t start = 0; start + ngram_token_size <= char_count; ++start)
        {
            const size_t begin = char_boundaries[start];
            const size_t end = char_boundaries[start + ngram_token_size];
            if (!enable_stopword || !containsNgramStopword(run, char_boundaries, start, ngram_token_size, stopwords))
            {
                result.append(
                    run.substr(begin, end - begin),
                    base_position + start,
                    ngram_token_size,
                    borrow_document && collator);
                if (!collator && preserve_case)
                    lowercaseTokenInPlace(result[result.size() - 1].text);
            }
        }
        next_position_base = base_position + char_count - ngram_token_size + 1;
    }
}

FullTextColumn analyzeNgramText(
    std::string_view text,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr,
    bool enable_stopword = true)
{
    FullTextColumn result;
    AnalyzerScratch scratch;
    analyzeNgramTextInto(text, result, scratch, ngram_token_size, collator, stopwords, enable_stopword);
    return result;
}

bool textEquals(std::string_view lhs, std::string_view rhs, const TiDB::TiDBCollatorPtr & collator)
{
    if (!collator)
        return lhs == rhs;
    return collator->compare(lhs.data(), lhs.size(), rhs.data(), rhs.size()) == 0;
}

String makePrefixPattern(std::string_view prefix)
{
    // Reuse the same collation-aware pattern implementation as LIKE. Escape
    // token characters that have LIKE meaning before appending the wildcard.
    String pattern;
    pattern.reserve(prefix.size() + 1);
    for (const char c : prefix)
    {
        if (c == '\\' || c == '%' || c == '_')
            pattern.push_back('\\');
        pattern.push_back(c);
    }
    pattern.push_back('%');
    return pattern;
}

std::unique_ptr<TiDB::ITiDBCollator::IPattern> compilePrefixMatcher(
    std::string_view prefix,
    const TiDB::TiDBCollatorPtr & collator)
{
    auto matcher = collator->pattern();
    matcher->compile(makePrefixPattern(prefix), '\\');
    return matcher;
}

bool textStartsWith(std::string_view value, std::string_view prefix, const TiDB::TiDBCollatorPtr & collator)
{
    if (!collator)
        return value.starts_with(prefix);
    const auto matcher = compilePrefixMatcher(prefix, collator);
    return matcher->match(value.data(), value.size());
}

bool matchesTermAt(
    const BooleanClause & clause,
    size_t term_index,
    const FullTextToken & token,
    const TiDB::TiDBCollatorPtr & collator)
{
    const auto value = token.value();
    if (!clause.prefix)
        return textEquals(value, clause.terms[term_index], collator);
    if (collator && term_index < clause.prefix_matchers.size())
        return clause.prefix_matchers[term_index]->match(value.data(), value.size());
    return textStartsWith(value, clause.terms[term_index], collator);
}

struct ClauseMatchState
{
    std::vector<UInt8> matched_terms;
    size_t remaining_terms = 0;
    bool matched = false;
    bool phrase_verification_failed = false;
};

void initializeClauseMatchStates(const std::vector<BooleanClause> & clauses, std::vector<ClauseMatchState> & states)
{
    states.resize(clauses.size());
    for (size_t i = 0; i < clauses.size(); ++i)
        if (!clauses[i].phrase)
        {
            states[i].matched_terms.resize(clauses[i].terms.size());
            states[i].remaining_terms = clauses[i].terms.size();
        }
}

bool matchesPhraseEndingAt(
    const BooleanClause & clause,
    const FullTextColumn & column,
    const FullTextToken & end,
    const TiDB::TiDBCollatorPtr & collator)
{
    if (clause.terms.empty() || clause.terms.size() != clause.offsets.size())
        return false;

    const size_t last_term = clause.terms.size() - 1;
    const size_t last_offset = clause.offsets[last_term];
    if (end.position < last_offset || !matchesTermAt(clause, last_term, end, collator))
        return false;

    const size_t start_position = end.position - last_offset;
    for (size_t i = 0; i < last_term; ++i)
    {
        const size_t expected_position = start_position + clause.offsets[i];
        const auto it = std::lower_bound(
            column.begin(),
            column.end(),
            expected_position,
            [](const FullTextToken & token, size_t position) { return token.position < position; });
        if (it == column.end() || it->position != expected_position
            || !textEquals(it->value(), clause.terms[i], collator))
            return false;
    }
    return true;
}

void analyzeColumnInto(
    std::string_view document,
    FullTextColumn & output,
    AnalyzerScratch & scratch,
    bool use_ngram,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    if (use_ngram)
        analyzeNgramTextInto(document, output, scratch, ngram_token_size, collator, stopwords, enable_stopword, true);
    else
        analyzeTextInto(document, output, collator, stopwords, min_token_size, max_token_size, enable_stopword, true);
}

void resetClauseMatchStates(std::vector<ClauseMatchState> & states)
{
    for (auto & state : states)
    {
        state.matched = false;
        state.remaining_terms = state.matched_terms.size();
        std::fill(state.matched_terms.begin(), state.matched_terms.end(), 0);
    }
}

struct BooleanMatchProgress
{
    size_t matched_must_clauses = 0;
    bool matched_should_clause = false;
};

enum class ColumnMatchResult
{
    Continue,
    Accepted,
    Rejected,
};

ColumnMatchResult updateBooleanMatchStatesForColumn(
    const CompiledBooleanQuery & query,
    const FullTextColumn & column,
    const FullTextColumn & unfiltered_column,
    const TiDB::TiDBCollatorPtr & collator,
    std::vector<ClauseMatchState> & states,
    BooleanMatchProgress & progress)
{
    const auto & clauses = query.clauses;
    if (query.matches_nothing || clauses.empty())
        return ColumnMatchResult::Continue;

    // Raw phrase verification searches the entire column, so a failed result
    // needs no recheck at later anchors. Unlike positive clause matches, this
    // negative cache belongs only to this column, not other MATCH columns.
    for (auto & state : states)
        state.phrase_verification_failed = false;

    const auto markClauseMatched = [&](size_t clause_index) {
        states[clause_index].matched = true;
        switch (clauses[clause_index].modifier)
        {
        case BooleanClause::Modifier::Must:
            ++progress.matched_must_clauses;
            break;
        case BooleanClause::Modifier::Should:
            progress.matched_should_clause = true;
            break;
        case BooleanClause::Modifier::MustNot:
            return ColumnMatchResult::Rejected;
        }
        if (!query.has_must_not
            && (query.must_clause_count != 0 ? progress.matched_must_clauses == query.must_clause_count
                                             : progress.matched_should_clause))
            return ColumnMatchResult::Accepted;
        return ColumnMatchResult::Continue;
    };

    // Walk this MATCH column's analyzed tokens once and update every clause's
    // state. Phrase clauses are checked only when their final term is
    // encountered; positional lookups preserve stopword gaps and phrases do
    // not cross MATCH columns.
    for (const auto & token : column)
    {
        for (size_t clause_index = 0; clause_index < clauses.size(); ++clause_index)
        {
            const auto & clause = clauses[clause_index];
            auto & state = states[clause_index];
            if (state.matched || state.phrase_verification_failed || clause.terms.empty())
                continue;

            if (clause.phrase)
            {
                if (matchesPhraseEndingAt(clause, column, token, collator))
                {
                    if (clause.verification
                        && !std::any_of(unfiltered_column.begin(), unfiltered_column.end(), [&](const auto & end) {
                               return matchesPhraseEndingAt(*clause.verification, unfiltered_column, end, collator);
                           }))
                    {
                        state.phrase_verification_failed = true;
                        continue;
                    }
                    const auto result = markClauseMatched(clause_index);
                    if (result != ColumnMatchResult::Continue)
                        return result;
                }
                continue;
            }

            for (size_t term_index = 0; term_index < clause.terms.size(); ++term_index)
                if (!state.matched_terms[term_index] && matchesTermAt(clause, term_index, token, collator))
                {
                    state.matched_terms[term_index] = 1;
                    --state.remaining_terms;
                }
            // STANDARD may split one SQL term into several words. Required
            // terms intersect those words, but optional/prohibited terms use
            // their union, exactly like TiDB's combineBooleanTermNodes.
            state.matched = clause.modifier == BooleanClause::Modifier::Must
                ? state.remaining_terms == 0
                : state.remaining_terms < clause.terms.size();
            if (state.matched)
            {
                const auto result = markClauseMatched(clause_index);
                if (result != ColumnMatchResult::Continue)
                    return result;
            }
        }
    }
    return ColumnMatchResult::Continue;
}

bool evaluateBooleanMatchResult(const CompiledBooleanQuery & query, const std::vector<ClauseMatchState> & states)
{
    if (query.matches_nothing || query.clauses.empty())
        return false;
    bool has_positive = false;
    bool has_must = false;
    bool positive_match = false;
    for (size_t i = 0; i < query.clauses.size(); ++i)
    {
        const auto & clause = query.clauses[i];
        const bool matched = states[i].matched;
        switch (clause.modifier)
        {
        case BooleanClause::Modifier::Must:
            has_must = true;
            if (!matched)
                return false;
            break;
        case BooleanClause::Modifier::MustNot:
            if (matched)
                return false;
            break;
        case BooleanClause::Modifier::Should:
            has_positive = true;
            positive_match = positive_match || matched;
            break;
        }
    }

    return has_must || !has_positive || positive_match;
}

CompiledBooleanQuery compileBooleanQuery(
    const tipb::LocalMatchAgainstBooleanQuery & query,
    bool use_ngram,
    size_t ngram_token_size,
    size_t min_token_size,
    size_t max_token_size,
    bool enable_stopword,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr)
{
    CompiledBooleanQuery compiled;
    if (!use_ngram && (max_token_size == 0 || min_token_size > max_token_size))
    {
        compiled.matches_nothing = true;
        return compiled;
    }
    auto analyze_query = [&](std::string_view text) {
        return use_ngram ? analyzeNgramText(text, ngram_token_size, collator, stopwords, enable_stopword)
                         : analyzeText(text, collator, stopwords, min_token_size, max_token_size, enable_stopword);
    };
    for (const auto & node : query.nodes())
    {
        BooleanClause clause;
        size_t first_query_position = 0;
        switch (node.occur())
        {
        case tipb::LocalMatchAgainstBooleanOccur::LocalMatchAgainstBooleanOccurMust:
            clause.modifier = BooleanClause::Modifier::Must;
            break;
        case tipb::LocalMatchAgainstBooleanOccur::LocalMatchAgainstBooleanOccurMustNot:
            clause.modifier = BooleanClause::Modifier::MustNot;
            break;
        case tipb::LocalMatchAgainstBooleanOccur::LocalMatchAgainstBooleanOccurShould:
            clause.modifier = BooleanClause::Modifier::Should;
            break;
        default:
            compiled.matches_nothing = true;
            return compiled;
        }

        switch (node.term_type())
        {
        case tipb::LocalMatchAgainstBooleanTermType::LocalMatchAgainstBooleanTermWord:
        {
            const auto terms = analyze_query(node.text());
            if (use_ngram)
            {
                clause.phrase = true;
                const size_t first_position = terms.empty() ? 0 : terms.front().position;
                first_query_position = first_position;
                for (const auto & token : terms)
                {
                    clause.terms.push_back(token.text);
                    clause.offsets.push_back(token.position - first_position);
                }
            }
            else
            {
                for (const auto & token : terms)
                    clause.terms.push_back(token.text);
            }
            break;
        }
        case tipb::LocalMatchAgainstBooleanTermType::LocalMatchAgainstBooleanTermPrefix:
        {
            const auto terms = use_ngram
                ? analyzeNgramText(node.text(), ngram_token_size, collator, stopwords, enable_stopword)
                : tokenizeText(node.text(), collator);
            if (use_ngram)
            {
                if (!terms.empty())
                {
                    // TiDB expands an NGRAM prefix at least as long as one
                    // gram into a positional phrase of query ngrams. Matching
                    // only one analyzed term here makes prefixes such as
                    // "caf*" ("ca", "af" at token size 2) never match.
                    clause.phrase = true;
                    const size_t first_position = terms.front().position;
                    first_query_position = first_position;
                    for (const auto & token : terms)
                    {
                        clause.terms.push_back(token.text);
                        clause.offsets.push_back(token.position - first_position);
                    }
                    break;
                }

                // A query shorter than the configured gram size is retained
                // by TiDB as a prefix over document ngrams instead of being
                // discarded by the analyzer.
                const auto source_terms = tokenizeText(node.text(), collator);
                if (source_terms.size() == 1)
                {
                    const auto code_points = UTF8::countCodePoints(
                        reinterpret_cast<const UInt8 *>(source_terms.front().text.data()),
                        source_terms.front().text.size());
                    if (code_points < ngram_token_size)
                    {
                        clause.prefix = true;
                        clause.terms.push_back(source_terms.front().text);
                        break;
                    }
                }

                if (clause.modifier == BooleanClause::Modifier::Must)
                    compiled.clauses.push_back(std::move(clause));
                continue;
            }

            clause.prefix = true;
            if (terms.size() != 1)
            {
                if (clause.modifier == BooleanClause::Modifier::Must)
                    compiled.clauses.push_back(std::move(clause));
                continue;
            }
            const auto code_points = UTF8::countCodePoints(
                reinterpret_cast<const UInt8 *>(terms.front().text.data()),
                terms.front().text.size());
            if (code_points > max_token_size)
            {
                if (clause.modifier == BooleanClause::Modifier::Must)
                    compiled.clauses.push_back(std::move(clause));
                continue;
            }
            clause.terms.push_back(terms.front().text);
            break;
        }
        case tipb::LocalMatchAgainstBooleanTermType::LocalMatchAgainstBooleanTermPhrase:
        {
            clause.phrase = true;
            const auto terms = analyze_query(node.text());
            if (use_ngram)
            {
                const auto words = tokenizeText(node.text(), collator);
                bool seen_indexed_word = false;
                if (std::any_of(words.begin(), words.end(), [&](const auto & word) {
                        if (word.code_points >= ngram_token_size)
                        {
                            seen_indexed_word = seen_indexed_word || !analyze_query(word.text).empty();
                            return false;
                        }
                        if (!enable_stopword || seen_indexed_word)
                            return true;
                        std::vector<size_t> boundaries{0};
                        for (size_t offset = 0; offset < word.text.size();)
                        {
                            offset += decodeCodePoint(word.text, offset).second;
                            boundaries.push_back(offset);
                        }
                        return !containsNgramStopword(word.text, boundaries, 0, word.code_points, stopwords);
                    }))
                    break; // A short query unigram cannot occur in the document's fixed-size ngrams.
            }
            const size_t first_position = terms.empty() ? 0 : terms.front().position;
            first_query_position = first_position;
            for (const auto & token : terms)
            {
                clause.terms.push_back(token.text);
                clause.offsets.push_back(token.position - first_position);
            }
            break;
        }
        default:
            compiled.matches_nothing = true;
            return compiled;
        }

        if (clause.phrase && !clause.terms.empty())
        {
            auto raw_terms = use_ngram
                ? analyzeNgramText(node.text(), ngram_token_size, collator, nullptr, false)
                : analyzeText(node.text(), collator, nullptr, 0, std::numeric_limits<size_t>::max(), false);
            // InnoDB discards leading filtered tokens, but retains every
            // following word in the original phrase used for verification.
            raw_terms.filterInPlace([&](const auto & token) { return token.position >= first_query_position; });
            if (raw_terms.size() != clause.terms.size())
            {
                // InnoDB verifies phrases against the original document, not
                // just the index tokens: removing "the" must not turn it into
                // an arbitrary positional gap. Ordinary terms/prefixes do not
                // pay for this additional document stream.
                clause.verification = std::make_unique<BooleanClause>();
                clause.verification->phrase = true;
                const size_t first = raw_terms.front().position;
                for (const auto & term : raw_terms)
                {
                    clause.verification->terms.push_back(term.text);
                    clause.verification->offsets.push_back(term.position - first);
                }
                compiled.needs_verification = true;
            }
        }

        if (clause.prefix && collator)
        {
            clause.prefix_matchers.reserve(clause.terms.size());
            for (const auto & term : clause.terms)
                clause.prefix_matchers.push_back(compilePrefixMatcher(term, collator));
        }

        if (!clause.terms.empty() || clause.modifier == BooleanClause::Modifier::Must)
            compiled.clauses.push_back(std::move(clause));
    }

    for (const auto & clause : compiled.clauses)
    {
        if (clause.modifier == BooleanClause::Modifier::Must)
        {
            // An analyzer-filtered required clause cannot match any document.
            // Do this after compiling ALL nodes so malformed later nodes
            // retain their existing no-match handling, rather than being
            // hidden by an earlier block-level shortcut.
            if (clause.terms.empty())
            {
                compiled.matches_nothing = true;
                return compiled;
            }
            ++compiled.must_clause_count;
        }
        else if (clause.modifier == BooleanClause::Modifier::MustNot)
            compiled.has_must_not = true;
    }

    // A BOOLEAN MODE query containing only prohibited terms has no positive
    // branch. TiDB's local evaluator treats it as matching no rows.
    if (std::none_of(compiled.clauses.begin(), compiled.clauses.end(), [](const BooleanClause & clause) {
            return clause.modifier != BooleanClause::Modifier::MustNot;
        }))
        compiled.matches_nothing = true;

    return compiled;
}

Float64 matchBooleanScore(const CompiledBooleanQuery & query, const std::vector<ClauseMatchState> & states)
{
    // The protocol path is the no-score MATCH ... AGAINST BOOLEAN MODE
    // predicate introduced by #70484/#70485. TiDB's local evaluator returns
    // a boolean 0/1 result, so do not expose term-frequency counts here.
    return evaluateBooleanMatchResult(query, states) ? 1 : 0;
}

class FullTextColumnAccessor
{
public:
    explicit FullTextColumnAccessor(const IColumn & column) { initialize(column, false); }

    bool isNull(size_t row) const
    {
        return nullable_column != nullptr && nullable_column->getNullMapData()[nullable_is_constant ? 0 : row] != 0;
    }

    std::string_view getString(size_t row) const
    {
        const auto & chars = string_column->getChars();
        const auto & offsets = string_column->getOffsets();
        const size_t string_row = string_is_constant ? 0 : row;
        const size_t begin = string_row == 0 ? 0 : offsets[string_row - 1];
        const size_t end = offsets[string_row];
        return {reinterpret_cast<const char *>(&chars[begin]), end - begin - 1};
    }

private:
    void initialize(const IColumn & column, bool row_is_constant)
    {
        if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
        {
            string_is_constant = true;
            initialize(column_const->getDataColumn(), true);
            return;
        }
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
        {
            nullable_column = column_nullable;
            nullable_is_constant = row_is_constant;
            initialize(column_nullable->getNestedColumn(), row_is_constant);
            return;
        }
        string_column = checkAndGetColumn<ColumnString>(&column);
        if (string_column == nullptr)
            throw Exception("Full-text arguments must be string columns", ErrorCodes::ILLEGAL_COLUMN);
    }

    const ColumnString * string_column = nullptr;
    const ColumnNullable * nullable_column = nullptr;
    bool string_is_constant = false;
    bool nullable_is_constant = false;
};

bool decodeLocalMatchAgainstBooleanQuery(const IColumn & column, tipb::LocalMatchAgainstBooleanQuery & query)
{
    const auto * constant = typeid_cast<const ColumnConst *>(&column);
    if (constant == nullptr)
        return false;
    const auto encoded = constant->getValue<String>();
    // Expr.val directly contains the versioned Local MATCH query. Validate the
    // semantic version before interpreting parser or stopword fields; never
    // guess a default for a version this TiFlash binary does not understand.
    if (!query.ParseFromString(encoded) || query.version() != local_match_against_protocol_version)
        return false;
    if (query.parser() != tipb::LocalMatchAgainstParser::LocalMatchAgainstParserStandard
        && query.parser() != tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram)
        return false;
    if (query.stopword_mode() != tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeDisabled
        && query.stopword_mode() != tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin)
        return false;
    return true;
}

class FunctionLocalMatchAgainstBoolean final : public IFunction
{
public:
    static constexpr auto name = "local_match_against_boolean";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLocalMatchAgainstBoolean>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return false; }
    // NULL MATCH columns are empty documents, not NULL MATCH results. The
    // implementation below handles nullable columns per row, so the generic
    // IFunction NULL wrapper must not short-circuit the whole expression.
    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception("local_match_against_boolean requires a query, at least one column, and metadata");
        bool nullable = false;
        for (const auto & argument : arguments)
        {
            if (!removeNullable(argument)->isString())
                throw Exception(
                    "Illegal type " + argument->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            nullable = nullable || argument->isNullable();
        }
        DataTypePtr result_type = std::make_shared<DataTypeFloat64>();
        return nullable ? std::make_shared<DataTypeNullable>(result_type) : result_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * query_column = typeid_cast<const ColumnConst *>(&*block.getByPosition(arguments[0]).column);
        if (query_column == nullptr)
            throw Exception(
                "The query argument of local_match_against_boolean must be constant",
                ErrorCodes::ILLEGAL_COLUMN);

        const size_t rows = block.getByPosition(arguments[1]).column->size();
        auto output = ColumnFloat64::create(rows, 0);
        auto & output_data = output->getData();
        const bool nullable = block.getByPosition(result).type->isNullable();
        auto null_map = nullable ? ColumnUInt8::create(rows, 0) : nullptr;

        const FullTextColumnAccessor query_accessor(*query_column);
        tipb::LocalMatchAgainstBooleanQuery protocol_boolean_query;
        if (arguments.size() <= 2
            || !decodeLocalMatchAgainstBooleanQuery(
                *block.getByPosition(arguments.back()).column,
                protocol_boolean_query))
            throw Exception(
                "local_match_against_boolean requires valid Boolean query metadata",
                ErrorCodes::ILLEGAL_COLUMN);
        if (query_accessor.isNull(0))
        {
            // InnoDB interprets NULL AGAINST as an empty search: non-NULL zero.
            ColumnPtr result_column;
            if (null_map)
                result_column = ColumnNullable::create(std::move(output), std::move(null_map));
            else
                result_column = std::move(output);
            block.getByPosition(result).column = std::move(result_column);
            return;
        }
        size_t document_argument_end = arguments.size() - 1;
        const bool use_ngram
            = protocol_boolean_query.parser() == tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram;
        const size_t ngram_token_size = !use_ngram || protocol_boolean_query.ngram_token_size() == 0
            ? default_ngram_token_size
            : protocol_boolean_query.ngram_token_size();
        const bool has_standard_config = protocol_boolean_query.innodb_ft_min_token_size() != 0
            || protocol_boolean_query.innodb_ft_max_token_size() != 0;
        const size_t min_token_size
            = has_standard_config ? protocol_boolean_query.innodb_ft_min_token_size() : default_min_token_size;
        const size_t max_token_size
            = has_standard_config ? protocol_boolean_query.innodb_ft_max_token_size() : default_max_token_size;
        const bool enable_stopword = protocol_boolean_query.stopword_mode()
            == tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin;
        const auto stopword_collator
            = enable_stopword ? TiDB::ITiDBCollator::getCollator(protocol_boolean_query.stopword_collation()) : nullptr;
        if (enable_stopword && !stopword_collator)
            throw Exception(
                "local_match_against_boolean requires a supported stopword collation when stopwords are enabled",
                ErrorCodes::ILLEGAL_COLUMN);
        std::optional<BuiltinStopwordLookup> stopword_lookup;
        if (enable_stopword)
            stopword_lookup.emplace(stopword_collator);
        const auto * stopwords = stopword_lookup ? &*stopword_lookup : nullptr;
        const CompiledBooleanQuery compiled_query = compileBooleanQuery(
            protocol_boolean_query,
            use_ngram,
            ngram_token_size,
            min_token_size,
            max_token_size,
            enable_stopword,
            collator,
            stopwords);
        if (compiled_query.matches_nothing)
        {
            ColumnPtr result_column;
            if (null_map)
                result_column = ColumnNullable::create(std::move(output), std::move(null_map));
            else
                result_column = std::move(output);
            block.getByPosition(result).column = std::move(result_column);
            return;
        }
        std::vector<FullTextColumnAccessor> document_columns;
        document_columns.reserve(document_argument_end - 1);
        for (size_t arg = 1; arg < document_argument_end; ++arg)
            document_columns.emplace_back(*block.getByPosition(arguments[arg]).column);
        std::vector<ClauseMatchState> clause_states;
        initializeClauseMatchStates(compiled_query.clauses, clause_states);
        FullTextColumn analyzed_column;
        FullTextColumn unfiltered_column;
        AnalyzerScratch analyzer_scratch;

        for (size_t row = 0; row < rows; ++row)
        {
            resetClauseMatchStates(clause_states);
            BooleanMatchProgress match_progress;
            bool result_known = false;
            bool row_matches = false;
            for (const auto & document_column : document_columns)
            {
                // A NULL MATCH column contributes no tokens. This is
                // intentional: #70485 relies on a row with a NULL body
                // still matching a required term from another MATCH column.
                if (document_column.isNull(row))
                {
                    analyzed_column.clear();
                    continue;
                }
                analyzeColumnInto(
                    document_column.getString(row),
                    analyzed_column,
                    analyzer_scratch,
                    use_ngram,
                    ngram_token_size,
                    collator,
                    stopwords,
                    min_token_size,
                    max_token_size,
                    enable_stopword);
                if (compiled_query.needs_verification)
                    analyzeColumnInto(
                        document_column.getString(row),
                        unfiltered_column,
                        analyzer_scratch,
                        use_ngram,
                        ngram_token_size,
                        collator,
                        nullptr,
                        0,
                        std::numeric_limits<size_t>::max(),
                        false);
                const auto match_result = updateBooleanMatchStatesForColumn(
                    compiled_query,
                    analyzed_column,
                    unfiltered_column,
                    collator,
                    clause_states,
                    match_progress);
                if (match_result == ColumnMatchResult::Rejected)
                {
                    result_known = true;
                    break;
                }
                if (match_result == ColumnMatchResult::Accepted)
                {
                    result_known = true;
                    row_matches = true;
                    break;
                }
            }
            output_data[row] = result_known ? (row_matches ? 1 : 0) : matchBooleanScore(compiled_query, clause_states);
        }
        ColumnPtr result_column;
        if (null_map)
            result_column = ColumnNullable::create(std::move(output), std::move(null_map));
        else
            result_column = std::move(output);
        block.getByPosition(result).column = std::move(result_column);
    }

private:
    TiDB::TiDBCollatorPtr collator;
};
} // namespace

void registerFunctionsLocalMatchAgainst(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLocalMatchAgainstBoolean>(FunctionFactory::CaseInsensitive);
}
} // namespace DB

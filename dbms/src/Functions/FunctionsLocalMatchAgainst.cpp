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
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsLocalMatchAgainst.h>
#include <Poco/UTF8String.h>
#include <Poco/Unicode.h>
#include <TiDB/Collation/Collator.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <memory>
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
// product version. Version 1 fixes the Boolean AST interpretation, analyzer
// behavior, and built-in stopword set. TiFlash must support a version before
// TiDB emits it; unknown versions are rejected, and existing versions must
// never be reinterpreted with changed semantics.
constexpr UInt32 local_match_against_protocol_version = 1;

struct FullTextToken
{
    String text;
    size_t position = 0;
    size_t code_points = 0;
};

class FullTextColumn
{
public:
    using Container = std::vector<FullTextToken>;
    using iterator = Container::iterator;
    using const_iterator = Container::const_iterator;

    void clear() { used = 0; }

    void reserve(size_t size) { tokens.reserve(size); }

    void append(std::string_view text, size_t position, size_t code_points)
    {
        if (used == tokens.size())
            tokens.emplace_back();
        auto & token = tokens[used++];
        token.text.assign(text.data(), text.size());
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
    std::vector<std::unique_ptr<TiDB::ITiDBCollator::IPattern>> prefix_matchers;
};

struct AnalyzerScratch
{
    FullTextColumn raw_tokens;
    std::vector<size_t> char_boundaries;
};

struct CompiledBooleanQuery
{
    std::vector<BooleanClause> clauses;
    bool matches_nothing = false;
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
    return isAlphaNumericASCII(static_cast<char>(code_point)) || code_point == '_' || Poco::Unicode::isAlpha(code_point)
        || Poco::Unicode::isDigit(code_point);
}

std::pair<UInt32, size_t> decodeCodePoint(std::string_view text, size_t offset)
{
    const auto decoded = UTF8::utf8Decode(text.data() + offset, text.size() - offset);
    if (decoded.second == 0 || decoded.first == UTF8::UTF8_Error || decoded.second > text.size() - offset)
        return {static_cast<UInt8>(text[offset]), 1};
    return {decoded.first, decoded.second};
}

void tokenizeTextInto(
    std::string_view text,
    FullTextColumn & result,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    bool preserve_case = false)
{
    result.clear();
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
        size_t code_points = 0;
        while (i < text.size())
        {
            const auto [code_point, length] = decodeCodePoint(text, i);
            if (!isFullTextToken(code_point))
                break;
            ++code_points;
            i += length;
        }

        // If a collation is present, retain the source spelling and let the
        // collator decide case and accent equivalence during matching. This
        // is important for binary collations, where lower-casing would make
        // a case-sensitive MATCH unexpectedly case-insensitive. Keep the
        // legacy lower-case behavior for callers without collation metadata.
        result.append(text.substr(token_start, i - token_start), position++, code_points);
        if (!collator && !preserve_case)
            lowercaseTokenInPlace(result[result.size() - 1].text);
    }
}

FullTextColumn tokenizeText(
    std::string_view text,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    bool preserve_case = false)
{
    FullTextColumn result;
    tokenizeTextInto(text, result, collator, preserve_case);
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

const std::unordered_map<size_t, std::vector<String>> & defaultStopwordsByLength()
{
    static const auto stopwords_by_length = [] {
        std::unordered_map<size_t, std::vector<String>> result;
        for (const auto & stopword : defaultStopwords())
        {
            const size_t length
                = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(stopword.data()), stopword.size());
            result[length].push_back(stopword);
        }
        return result;
    }();
    return stopwords_by_length;
}

bool isDefaultStopword(std::string_view token, const TiDB::TiDBCollatorPtr & stopword_collator)
{
    const auto & stopwords = defaultStopwords();
    if (!stopword_collator)
        return stopwords.contains(Poco::UTF8::toLower(String(token)));
    // MySQL compares stopwords using collation_server. This is independent of
    // the MATCH column collation and may be case-sensitive (for example
    // utf8mb4_bin).
    return std::any_of(stopwords.begin(), stopwords.end(), [&](const String & stopword) {
        return stopword_collator->compare(token.data(), token.size(), stopword.data(), stopword.size()) == 0;
    });
}

bool containsNgramStopword(
    std::string_view token,
    const std::vector<size_t> & char_boundaries,
    size_t ngram_start,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & stopword_collator)
{
    const size_t ngram_end = ngram_start + ngram_token_size;
    const auto & stopwords = defaultStopwords();
    const auto & stopwords_by_length = defaultStopwordsByLength();
    for (size_t start = ngram_start; start < ngram_end; ++start)
    {
        for (size_t end = start + 1; end <= ngram_end; ++end)
        {
            const auto candidate = token.substr(char_boundaries[start], char_boundaries[end] - char_boundaries[start]);
            if (!stopword_collator && stopwords.contains(Poco::UTF8::toLower(String(candidate))))
                return true;
            if (stopword_collator)
            {
                const auto found = stopwords_by_length.find(end - start);
                if (found != stopwords_by_length.end()
                    && std::any_of(found->second.begin(), found->second.end(), [&](const String & stopword) {
                           return stopword_collator
                                      ->compare(candidate.data(), candidate.size(), stopword.data(), stopword.size())
                               == 0;
                       }))
                    return true;
            }
        }
    }
    return false;
}

void analyzeTextInto(
    std::string_view text,
    FullTextColumn & result,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const TiDB::TiDBCollatorPtr & stopword_collator = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    const bool preserve_case = enable_stopword && static_cast<bool>(stopword_collator);
    tokenizeTextInto(text, result, collator, preserve_case);
    result.filterInPlace([&](const FullTextToken & token) {
        return token.code_points >= min_token_size && token.code_points <= max_token_size
            && (!enable_stopword || !isDefaultStopword(token.text, stopword_collator));
    });
    // When stopwords are compared with collation_server, tokenization must
    // retain source case until filtering is complete. MATCH without an
    // explicit column collation still uses the existing case-insensitive
    // behavior afterwards.
    if (!collator && preserve_case)
        for (auto & token : result)
            lowercaseTokenInPlace(token.text);
}

FullTextColumn analyzeText(
    std::string_view text,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const TiDB::TiDBCollatorPtr & stopword_collator = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    FullTextColumn result;
    analyzeTextInto(text, result, collator, stopword_collator, min_token_size, max_token_size, enable_stopword);
    return result;
}

void analyzeNgramTextInto(
    std::string_view text,
    FullTextColumn & result,
    AnalyzerScratch & scratch,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const TiDB::TiDBCollatorPtr & stopword_collator = nullptr,
    bool enable_stopword = true)
{
    result.clear();
    if (ngram_token_size == 0)
        return;

    const bool preserve_case = enable_stopword && static_cast<bool>(stopword_collator);
    tokenizeTextInto(text, scratch.raw_tokens, collator, preserve_case);
    size_t next_position_base = 0;
    auto & char_boundaries = scratch.char_boundaries;
    for (auto & token : scratch.raw_tokens)
    {
        char_boundaries.clear();
        char_boundaries.reserve(token.code_points + 1);
        char_boundaries.push_back(0);
        for (size_t offset = 0; offset < token.text.size();)
        {
            const auto [code_point, length] = decodeCodePoint(token.text, offset);
            (void)code_point;
            offset += length;
            char_boundaries.push_back(offset);
        }

        const size_t char_count = token.code_points;
        const size_t base_position = std::max(token.position, next_position_base);
        if (char_count < ngram_token_size)
        {
            next_position_base = std::max(next_position_base, token.position + 1);
            continue;
        }

        for (size_t start = 0; start + ngram_token_size <= char_count; ++start)
        {
            const size_t begin = char_boundaries[start];
            const size_t end = char_boundaries[start + ngram_token_size];
            if (!enable_stopword
                || !containsNgramStopword(token.text, char_boundaries, start, ngram_token_size, stopword_collator))
            {
                result.append(
                    std::string_view(token.text).substr(begin, end - begin),
                    base_position + start,
                    ngram_token_size);
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
    const TiDB::TiDBCollatorPtr & stopword_collator = nullptr,
    bool enable_stopword = true)
{
    FullTextColumn result;
    AnalyzerScratch scratch;
    analyzeNgramTextInto(text, result, scratch, ngram_token_size, collator, stopword_collator, enable_stopword);
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
    if (!clause.prefix)
        return textEquals(token.text, clause.terms[term_index], collator);
    if (collator && term_index < clause.prefix_matchers.size())
        return clause.prefix_matchers[term_index]->match(token.text.data(), token.text.size());
    return textStartsWith(token.text, clause.terms[term_index], collator);
}

struct ClauseMatchState
{
    std::vector<UInt8> matched_terms;
    size_t remaining_terms = 0;
    bool matched = false;
};

void initializeClauseMatchStates(
    const std::vector<BooleanClause> & clauses,
    std::vector<ClauseMatchState> & states)
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
        if (it == column.end() || it->position != expected_position || !textEquals(it->text, clause.terms[i], collator))
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
    const TiDB::TiDBCollatorPtr & stopword_collator = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    if (use_ngram)
        analyzeNgramTextInto(
            document, output, scratch, ngram_token_size, collator, stopword_collator, enable_stopword);
    else
        analyzeTextInto(
            document, output, collator, stopword_collator, min_token_size, max_token_size, enable_stopword);
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

bool updateBooleanMatchStatesForColumn(
    const CompiledBooleanQuery & query,
    const FullTextColumn & column,
    const TiDB::TiDBCollatorPtr & collator,
    std::vector<ClauseMatchState> & states)
{
    const auto & clauses = query.clauses;
    if (query.matches_nothing || clauses.empty())
        return false;

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
            if (state.matched || clause.terms.empty())
                continue;

            if (clause.phrase)
            {
                if (matchesPhraseEndingAt(clause, column, token, collator))
                    state.matched = true;
                continue;
            }

            for (size_t term_index = 0; term_index < clause.terms.size(); ++term_index)
                if (!state.matched_terms[term_index] && matchesTermAt(clause, term_index, token, collator))
                {
                    state.matched_terms[term_index] = 1;
                    --state.remaining_terms;
                }
            state.matched = state.remaining_terms == 0;
            if (state.matched && clause.modifier == BooleanClause::Modifier::MustNot)
                return true;
        }
    }
    return false;
}

bool evaluateBooleanMatchResult(
    const CompiledBooleanQuery & query,
    const std::vector<ClauseMatchState> & states)
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
    const TiDB::TiDBCollatorPtr & stopword_collator = nullptr)
{
    CompiledBooleanQuery compiled;
    if (!use_ngram && (max_token_size == 0 || min_token_size > max_token_size))
    {
        compiled.matches_nothing = true;
        return compiled;
    }
    auto analyze_query = [&](std::string_view text) {
        return use_ngram
            ? analyzeNgramText(text, ngram_token_size, collator, stopword_collator, enable_stopword)
            : analyzeText(text, collator, stopword_collator, min_token_size, max_token_size, enable_stopword);
    };
    for (const auto & node : query.nodes())
    {
        BooleanClause clause;
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
                ? analyzeNgramText(node.text(), ngram_token_size, collator, stopword_collator, enable_stopword)
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
            const size_t first_position = terms.empty() ? 0 : terms.front().position;
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

        if (clause.prefix && collator)
        {
            clause.prefix_matchers.reserve(clause.terms.size());
            for (const auto & term : clause.terms)
                clause.prefix_matchers.push_back(compilePrefixMatcher(term, collator));
        }

        if (!clause.terms.empty() || clause.modifier == BooleanClause::Modifier::Must)
            compiled.clauses.push_back(std::move(clause));
    }

    // A BOOLEAN MODE query containing only prohibited terms has no positive
    // branch. TiDB's local evaluator treats it as matching no rows.
    if (std::none_of(compiled.clauses.begin(), compiled.clauses.end(), [](const BooleanClause & clause) {
            return clause.modifier != BooleanClause::Modifier::MustNot;
        }))
        compiled.matches_nothing = true;

    return compiled;
}

Float64 matchBooleanScore(
    const CompiledBooleanQuery & query,
    const std::vector<ClauseMatchState> & states)
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
        return nullable_column != nullptr
            && nullable_column->getNullMapData()[nullable_is_constant ? 0 : row] != 0;
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
        auto * null_map_data = null_map ? &null_map->getData() : nullptr;

        const FullTextColumnAccessor query_accessor(*query_column);
        if (query_accessor.isNull(0))
        {
            if (null_map_data != nullptr)
                std::fill(null_map_data->begin(), null_map_data->end(), 1);
            ColumnPtr result_column;
            if (null_map)
                result_column = ColumnNullable::create(std::move(output), std::move(null_map));
            else
                result_column = std::move(output);
            block.getByPosition(result).column = std::move(result_column);
            return;
        }
        size_t document_argument_end = arguments.size() - 1;
        tipb::LocalMatchAgainstBooleanQuery protocol_boolean_query;
        if (arguments.size() <= 2
            || !decodeLocalMatchAgainstBooleanQuery(
                *block.getByPosition(arguments.back()).column,
                protocol_boolean_query))
            throw Exception(
                "local_match_against_boolean requires valid Boolean query metadata",
                ErrorCodes::ILLEGAL_COLUMN);
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
        const bool enable_stopword
            = protocol_boolean_query.stopword_mode()
            == tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin;
        const auto stopword_collator
            = enable_stopword ? TiDB::ITiDBCollator::getCollator(protocol_boolean_query.stopword_collation()) : nullptr;
        if (enable_stopword && !stopword_collator)
            throw Exception(
                "local_match_against_boolean requires a supported stopword collation when stopwords are enabled",
                ErrorCodes::ILLEGAL_COLUMN);
        const CompiledBooleanQuery compiled_query = compileBooleanQuery(
            protocol_boolean_query,
            use_ngram,
            ngram_token_size,
            min_token_size,
            max_token_size,
            enable_stopword,
            collator,
            stopword_collator);
        std::vector<FullTextColumnAccessor> document_columns;
        document_columns.reserve(document_argument_end - 1);
        for (size_t arg = 1; arg < document_argument_end; ++arg)
            document_columns.emplace_back(*block.getByPosition(arguments[arg]).column);
        std::vector<ClauseMatchState> clause_states;
        initializeClauseMatchStates(compiled_query.clauses, clause_states);
        FullTextColumn analyzed_column;
        AnalyzerScratch analyzer_scratch;

        for (size_t row = 0; row < rows; ++row)
        {
            resetClauseMatchStates(clause_states);
            bool rejected_by_prohibited_clause = false;
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
                    stopword_collator,
                    min_token_size,
                    max_token_size,
                    enable_stopword);
                if (updateBooleanMatchStatesForColumn(compiled_query, analyzed_column, collator, clause_states))
                {
                    rejected_by_prohibited_clause = true;
                    break;
                }
            }
            output_data[row] = rejected_by_prohibited_clause ? 0 : matchBooleanScore(compiled_query, clause_states);
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

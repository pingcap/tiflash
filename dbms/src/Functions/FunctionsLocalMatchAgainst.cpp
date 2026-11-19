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
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/MemoryTracker.h>
#include <Common/PODArray.h>
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
#include <common/defines.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
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
// characters. NGRAM queries retain short words for collation-aware matching.
// Filtered phrases also verify the unfiltered document stream.
constexpr UInt32 local_match_against_protocol_version = 2;

struct FullTextToken
{
    // Query terms and the no-collator lowercase path own their spelling.
    // Document views refer only to the current input column, which outlives
    // both analyzed streams and matching. Never retain them across blocks.
    String text;
    std::string_view source;
    size_t position = 0;

    std::string_view value() const { return source.data() ? source : std::string_view{text}; }
};

class FullTextColumn
{
public:
    using Container = std::vector<FullTextToken>;
    using iterator = Container::iterator;
    using const_iterator = Container::const_iterator;

    void clear() { used = 0; }

    void reserve(size_t size) { tokens.reserve(size); }

    // This is per-token work. Keep it inline even when the surrounding
    // analyzer grows: outlining it penalizes the unfiltered/OFF stream too.
    ALWAYS_INLINE void append(std::string_view text, size_t position, bool borrow = false)
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
    std::unique_ptr<TiDB::ITiDBCollator::IPattern> prefix_matcher;
};

struct AnalyzerScratch
{
    std::vector<size_t> char_boundaries;
    String lowercase_run;
};

// Short first-character misses and long UCA expansions can make key creation
// costlier than a small linear scan, even above the document token threshold.
// Reserve automatic routing for large queries; forced routing stays test-only.
constexpr size_t exact_route_min_terms = 100;
constexpr size_t exact_route_min_terms_large_block = 64;
constexpr size_t exact_route_min_tokens = 32;
constexpr size_t exact_route_min_tokens_large_block = 16;
constexpr size_t exact_route_large_block_min_rows = 128;
constexpr size_t exact_route_budget = 4 * 1024 * 1024;
constexpr size_t exact_route_max_token_bytes = 4096;

// sortKey's reusable std::string is not allocated through DB::Allocator.
// Account its heap capacity separately; all persistent route storage below
// uses the existing tracked Arena/HashMap/PODArray allocators.
class MatchKeyBuffer
{
public:
    MatchKeyBuffer() = default;
    MatchKeyBuffer(const MatchKeyBuffer &) = delete;
    MatchKeyBuffer & operator=(const MatchKeyBuffer &) = delete;
    ~MatchKeyBuffer() { CurrentMemoryTracker::free(charged); }
    StringRef key(std::string_view text, const TiDB::TiDBCollatorPtr & collator)
    {
        const auto result = collator->sortKeyFastPath(text.data(), text.size(), buffer);
        const size_t bytes = buffer.capacity() > String{}.capacity() ? buffer.capacity() + 1 : 0;
        if (bytes > charged)
        {
            CurrentMemoryTracker::alloc(bytes - charged);
            charged = bytes;
        }
        return result;
    }

private:
    String buffer;
    size_t charged = 0;
};

struct ExactTermRoute
{
    struct Reference
    {
        size_t bucket;
        size_t clause;
        size_t term;
    };
    struct Bucket
    {
        size_t offset;
        size_t count;
    };
    // Keys own their bytes, including empty keys and embedded zero weights.
    Arena keys;
    HashMap<StringRef, size_t, StringRefHash> dictionary;
    PODArray<Bucket> buckets;
    PODArray<Reference> references;
};

struct ExactMatchScratch
{
    PODArray<UInt32> seen;
    UInt32 generation;
    MatchKeyBuffer keys;

    ExactMatchScratch(size_t buckets, UInt32 initial_generation)
        : generation(initial_generation)
    {
        seen.resize_fill(buckets, 0);
    }
    void nextRow()
    {
        if (++generation == 0)
        {
            std::fill(seen.begin(), seen.end(), 0);
            generation = 1;
        }
    }
};

struct CompiledBooleanQuery
{
    std::vector<BooleanClause> clauses;
    size_t must_clause_count = 0;
    bool has_must_not = false;
    std::unique_ptr<ExactTermRoute> exact_route;
};

// Normalize the already validated wire configuration once per block. Keep
// the tokenizer implementations and their filtered/unfiltered rules intact.
struct LocalMatchAnalyzerConfig
{
    bool use_ngram;
    size_t ngram_token_size;
    size_t min_token_size;
    size_t max_token_size;
    bool enable_stopword;

    explicit LocalMatchAnalyzerConfig(const tipb::LocalMatchAgainstBooleanQuery & query)
        : use_ngram(query.parser() == tipb::LocalMatchAgainstParserNgram)
        , ngram_token_size(
              !use_ngram || query.ngram_token_size() == 0 ? default_ngram_token_size : query.ngram_token_size())
        , min_token_size(
              query.innodb_ft_min_token_size() != 0 || query.innodb_ft_max_token_size() != 0
                  ? query.innodb_ft_min_token_size()
                  : default_min_token_size)
        , max_token_size(
              query.innodb_ft_min_token_size() != 0 || query.innodb_ft_max_token_size() != 0
                  ? query.innodb_ft_max_token_size()
                  : default_max_token_size)
        , enable_stopword(query.stopword_mode() == tipb::LocalMatchAgainstStopwordModeBuiltin)
    {}
};

// Query tokens own their text. This cold-path helper only collects the same
// terms and source-relative offsets; document views never enter it.
size_t collectClauseTerms(BooleanClause & clause, const FullTextColumn & tokens)
{
    const size_t first_position = tokens.empty() ? 0 : tokens.front().position;
    for (const auto & token : tokens)
    {
        clause.terms.push_back(token.text);
        if (clause.phrase)
            clause.offsets.push_back(token.position - first_position);
    }
    return first_position;
}

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
    scanStandardTokens(text, [&](std::string_view token, size_t position, size_t) {
        result.append(token, position);
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

constexpr auto builtin_stopwords = std::to_array<std::string_view>(
    {"a",    "about", "an",  "are",  "as",   "at",    "be",  "by",   "com",  "de",  "en",   "for",
     "from", "how",   "i",   "in",   "is",   "it",    "la",  "of",   "on",   "or",  "that", "the",
     "this", "to",    "was", "what", "when", "where", "who", "will", "with", "und", "www"});

// The built-in list is ASCII, so byte and code-point lengths agree.
constexpr size_t max_builtin_stopword_length = [] {
    size_t result = 0;
    for (const auto word : builtin_stopwords)
        result = std::max(result, word.size());
    return result;
}();
static_assert(max_builtin_stopword_length < std::numeric_limits<UInt8>::digits);

const std::unordered_set<String> & defaultStopwords()
{
    static const auto stopwords = [] {
        std::unordered_set<String> result;
        result.reserve(builtin_stopwords.size());
        for (const auto word : builtin_stopwords)
            result.emplace(word);
        return result;
    }();
    return stopwords;
}

// Collation keys own their bytes and are reused across the block. Use the
// stopword (server) collation independently of the MATCH column collation.
class BuiltinStopwordLookup
{
public:
    explicit BuiltinStopwordLookup(TiDB::TiDBCollatorPtr collator_)
        : collator(collator_)
        , standard_length_limit(standardLengthLimit(collator_->getCollatorType()))
    {
        for (const auto stopword : builtin_stopwords)
        {
            const auto key = collator->sortKeyFastPath(stopword.data(), stopword.size(), key_buffer);
            max_key_size = std::max(max_key_size, key.size);
            auto * entry = keys.find(key);
            if (!entry)
            {
                // The sort key may reference the reusable buffer.
                const StringRef owned(key_storage.insert(key.data, key.size), key.size);
                bool inserted;
                keys.emplace(owned, entry, inserted);
                entry->getMapped() = 0;
            }
            entry->getMapped() |= lengthBit(stopword.size());
        }
    }

    bool containsStandard(std::string_view token, size_t code_points) const
    {
        const size_t length = standard_length_limit.count_code_points ? code_points : token.size();
        return length <= standard_length_limit.maximum && matchingLengths(token) != 0;
    }

    bool containsNgram(std::string_view token, size_t code_points) const
    {
        // NGRAM also requires the original code-point count, even when a
        // collation makes words of different lengths share a key.
        return code_points <= max_builtin_stopword_length && (matchingLengths(token) & lengthBit(code_points)) != 0;
    }

private:
    struct StandardLengthLimit
    {
        size_t maximum;
        bool count_code_points;
    };

    static StandardLengthLimit standardLengthLimit(TiDB::ITiDBCollator::CollatorType type)
    {
        using Type = TiDB::ITiDBCollator::CollatorType;
        switch (type)
        {
        case Type::UTF8MB4_BIN:
        case Type::UTF8_BIN:
        case Type::LATIN1_BIN:
        case Type::ASCII_BIN:
        case Type::BINARY:
        case Type::UTF8MB4_0900_BIN:
            // STANDARD tokens have no trailing spaces, so PAD SPACE cannot shorten them.
            return {max_builtin_stopword_length, false};
        case Type::UTF8_GENERAL_CI:
        case Type::UTF8MB4_GENERAL_CI:
            // General CI emits exactly one weight per code point.
            return {max_builtin_stopword_length, true};
        default:
            // Collations with ignorable weights can match longer source tokens.
            return {std::numeric_limits<size_t>::max(), false};
        }
    }

    static UInt8 lengthBit(size_t length) { return static_cast<UInt8>(1U << length); }

    struct CacheEntry
    {
        UInt64 bytes = 0;
        UInt8 size = 0;
        UInt8 lengths = 0;
    };

    UInt8 matchingLengths(std::string_view token) const
    {
        const size_t size = token.size();
        if (size == 0 || size > cache_token_bytes)
            return lookupMatchingLengths(token);

        // The first and last loads cover the complete token. The size
        // disambiguates equal packed values from tokens of different lengths.
        const char * data = token.data();
        UInt64 bytes;
        if (size >= 4)
            bytes = (static_cast<UInt64>(unalignedLoad<UInt32>(data)) << 32) | unalignedLoad<UInt32>(data + size - 4);
        else if (size >= 2)
            bytes = (static_cast<UInt64>(unalignedLoad<UInt16>(data)) << 16) | unalignedLoad<UInt16>(data + size - 2);
        else
            bytes = static_cast<UInt8>(*data);

        auto & entry = cache[intHashCRC32(bytes) & (cache.size() - 1)];
        const bool hit = entry.size == size && entry.bytes == bytes;
        if (unlikely(cache_samples_remaining != 0))
        {
            cache_sample_hits += hit;
            if (--cache_samples_remaining == 0 && cache_sample_hits < cache.size() / 4)
                cache_token_bytes = 0;
        }
        if (!hit)
            entry = {bytes, static_cast<UInt8>(size), lookupMatchingLengths(token)};
        return entry.lengths;
    }

    UInt8 lookupMatchingLengths(std::string_view token) const
    {
        const auto key = collator->sortKeyFastPath(token.data(), token.size(), key_buffer);
        if (key.size > max_key_size)
            return 0;
        const auto * entry = keys.find(key);
        return entry ? entry->getMapped() : 0;
    }

    TiDB::TiDBCollatorPtr collator;
    const StandardLengthLimit standard_length_limit;
    Arena key_storage;
    HashMap<StringRef, UInt8, StringRefHash, HashTableGrower<7>> keys;
    size_t max_key_size = 0;
    mutable String key_buffer;
    // A lookup and its collation are local to one block. Cache exact source
    // bytes, including misses, without keeping references to document data.
    mutable std::array<CacheEntry, 256> cache;
    // Sample the first cache-sized batch of short tokens. Avoid the cost of
    // probing the cache for the rest of the block when most probes miss.
    mutable size_t cache_samples_remaining = cache.size();
    mutable size_t cache_sample_hits = 0;
    mutable size_t cache_token_bytes = sizeof(UInt64);
};

bool isDefaultStopword(std::string_view token, size_t code_points, const BuiltinStopwordLookup * stopwords)
{
    return stopwords ? stopwords->containsStandard(token, code_points)
                     : defaultStopwords().contains(Poco::UTF8::toLower(String(token)));
}

bool containsNgramStopword(
    std::string_view token,
    const std::vector<size_t> & char_boundaries,
    const BuiltinStopwordLookup * stopwords)
{
    const size_t char_count = char_boundaries.size() - 1;
    for (size_t start = 0; start < char_count; ++start)
    {
        for (size_t end = start + 1; end <= char_count; ++end)
        {
            const auto candidate = token.substr(char_boundaries[start], char_boundaries[end] - char_boundaries[start]);
            if (!stopwords && defaultStopwords().contains(Poco::UTF8::toLower(String(candidate))))
                return true;
            if (stopwords && stopwords->containsNgram(candidate, end - start))
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
            || (enable_stopword && isDefaultStopword(token, code_points, stopwords)))
            return;
        result.append(token, position, borrow_document && collator);
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

enum class NgramAnalysisMode
{
    Document,
    Query,
    Prefix,
};

void analyzeNgramTextInto(
    std::string_view text,
    FullTextColumn & result,
    AnalyzerScratch & scratch,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator,
    const BuiltinStopwordLookup * stopwords,
    bool enable_stopword,
    NgramAnalysisMode mode)
{
    result.clear();
    if (ngram_token_size == 0)
        return;

    const bool borrow_tokens = mode == NgramAnalysisMode::Document && collator;
    const bool preserve_case = enable_stopword && stopwords;
    const size_t max_stopword_length = enable_stopword ? std::min(ngram_token_size, max_builtin_stopword_length) : 0;
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
            // A Boolean query retains a short word for collation-aware
            // lookup. The document stream still has only fixed-size grams.
            if (mode == NgramAnalysisMode::Query)
            {
                if (!enable_stopword || !containsNgramStopword(run, char_boundaries, stopwords))
                {
                    result.append(run, base_position);
                    if (!collator)
                        lowercaseTokenInPlace(result[result.size() - 1].text);
                }
                ++next_position_base;
            }
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

        const auto append_gram = [&](size_t start) ALWAYS_INLINE {
            const size_t begin = char_boundaries[start];
            const size_t end = char_boundaries[start + ngram_token_size];
            result.append(run.substr(begin, end - begin), base_position + start, borrow_tokens);
            if (!collator && preserve_case)
                lowercaseTokenInPlace(result[result.size() - 1].text);
        };

        // Keep the unfiltered stream and stopwords-OFF path free of per-gram
        // stopword branches and sliding-state dependencies.
        if (!enable_stopword)
        {
            for (size_t start = 0; start + ngram_token_size <= char_count; ++start)
                append_gram(start);
            next_position_base = base_position + char_count - ngram_token_size + 1;
            continue;
        }

        // Advance candidate endings once per run, instead of enumerating all
        // substrings again for every overlapping gram. Keep one past the
        // greatest start of a matched stopword ending at/before the gram end.
        // The gram [start, end) is filtered iff that value is greater than
        // start: such a stopword lies wholly inside the current gram. Once
        // start advances beyond it, it expires without a queue or cache.
        size_t stopword_end = 0;
        size_t filtered_until_start = 0;
        for (size_t start = 0; start + ngram_token_size <= char_count; ++start)
        {
            while (stopword_end < start + ngram_token_size)
            {
                ++stopword_end;
                const size_t max_length = std::min(stopword_end, max_stopword_length);
                for (size_t length = 1; length <= max_length; ++length)
                {
                    const size_t candidate_start = stopword_end - length;
                    // Longer candidates at this ending have earlier starts
                    // and cannot improve a start already known to match.
                    if (candidate_start < filtered_until_start)
                        break;
                    const auto candidate = run.substr(
                        char_boundaries[candidate_start],
                        char_boundaries[stopword_end] - char_boundaries[candidate_start]);
                    const bool matched = stopwords
                        ? stopwords->containsNgram(candidate, length)
                        : defaultStopwords().contains(Poco::UTF8::toLower(String(candidate)));
                    if (matched)
                    {
                        filtered_until_start = candidate_start + 1;
                        break;
                    }
                }
            }
            if (filtered_until_start <= start)
                append_gram(start);
        }
        next_position_base = base_position + char_count - ngram_token_size + 1;
    }
}

FullTextColumn analyzeNgramText(
    std::string_view text,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator,
    const BuiltinStopwordLookup * stopwords,
    bool enable_stopword,
    NgramAnalysisMode mode)
{
    FullTextColumn result;
    AnalyzerScratch scratch;
    analyzeNgramTextInto(text, result, scratch, ngram_token_size, collator, stopwords, enable_stopword, mode);
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

// This call is per unmatched term/token, including small-query fallbacks.
ALWAYS_INLINE bool matchesTermAt(
    const BooleanClause & clause,
    size_t term_index,
    const FullTextToken & token,
    const TiDB::TiDBCollatorPtr & collator)
{
    const auto value = token.value();
    if (!clause.prefix)
        return textEquals(value, clause.terms[term_index], collator);
    if (clause.prefix_matcher)
        return clause.prefix_matcher->match(value.data(), value.size());
    return value.starts_with(clause.terms[term_index]);
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

// Preserve the baseline's inlining after adding indexed/test call sites.
ALWAYS_INLINE bool matchesPhraseEndingAt(
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
        analyzeNgramTextInto(
            document,
            output,
            scratch,
            ngram_token_size,
            collator,
            stopwords,
            enable_stopword,
            NgramAnalysisMode::Document);
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

enum class ColumnMatchResult
{
    Continue,
    Accepted,
    Rejected,
};

ALWAYS_INLINE ColumnMatchResult markClauseMatched(
    const CompiledBooleanQuery & query,
    size_t index,
    std::vector<ClauseMatchState> & states,
    size_t & matched_must_clauses)
{
    states[index].matched = true;
    switch (query.clauses[index].modifier)
    {
    case BooleanClause::Modifier::Must:
        ++matched_must_clauses;
        break;
    case BooleanClause::Modifier::Should:
        break;
    case BooleanClause::Modifier::MustNot:
        return ColumnMatchResult::Rejected;
    }
    if (!query.has_must_not && matched_must_clauses == query.must_clause_count)
        return ColumnMatchResult::Accepted;
    return ColumnMatchResult::Continue;
}

ALWAYS_INLINE ColumnMatchResult markTermMatched(
    const CompiledBooleanQuery & query,
    size_t clause,
    size_t term,
    std::vector<ClauseMatchState> & states,
    size_t & matched_must_clauses)
{
    auto & state = states[clause];
    if (state.matched || state.matched_terms[term])
        return ColumnMatchResult::Continue;
    state.matched_terms[term] = 1;
    --state.remaining_terms;
    // A STANDARD SQL word may split into several terms: MUST intersects them,
    // while SHOULD/MUST NOT use their union. Both matchers share this rule.
    if (query.clauses[clause].modifier != BooleanClause::Modifier::Must || state.remaining_terms == 0)
        return markClauseMatched(query, clause, states, matched_must_clauses);
    return ColumnMatchResult::Continue;
}

template <typename GetUnfilteredColumn>
ALWAYS_INLINE ColumnMatchResult updateBooleanMatchStatesForColumn(
    const CompiledBooleanQuery & query,
    const FullTextColumn & column,
    GetUnfilteredColumn && get_unfiltered_column,
    const TiDB::TiDBCollatorPtr & collator,
    std::vector<ClauseMatchState> & states,
    size_t & matched_must_clauses)
{
    const auto & clauses = query.clauses;

    // Raw phrase verification searches the entire column, so a failed result
    // needs no recheck at later anchors. Unlike positive clause matches, this
    // negative cache belongs only to this column, not other MATCH columns.
    for (auto & state : states)
        state.phrase_verification_failed = false;

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
                    if (clause.verification)
                    {
                        const auto & unfiltered_column = get_unfiltered_column();
                        if (!std::any_of(unfiltered_column.begin(), unfiltered_column.end(), [&](const auto & end) {
                                return matchesPhraseEndingAt(*clause.verification, unfiltered_column, end, collator);
                            }))
                        {
                            state.phrase_verification_failed = true;
                            continue;
                        }
                    }
                    const auto result = markClauseMatched(query, clause_index, states, matched_must_clauses);
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
            state.matched = clause.modifier == BooleanClause::Modifier::Must
                ? state.remaining_terms == 0
                : state.remaining_terms < clause.terms.size();
            if (state.matched)
            {
                const auto result = markClauseMatched(query, clause_index, states, matched_must_clauses);
                if (result != ColumnMatchResult::Continue)
                    return result;
            }
        }
    }
    return ColumnMatchResult::Continue;
}

template <bool count_work>
ColumnMatchResult updateExactMatchStatesForColumn(
    const CompiledBooleanQuery & query,
    const FullTextColumn & column,
    const TiDB::TiDBCollatorPtr & collator,
    std::vector<ClauseMatchState> & states,
    size_t & matched_must_clauses,
    ExactMatchScratch & scratch,
    LocalMatchAgainstTest::Work * work)
{
    const auto & route = *query.exact_route;
    for (const auto & token : column)
    {
        if constexpr (count_work)
            ++work->key_lookups;
        const auto * const it = route.dictionary.find(scratch.keys.key(token.value(), collator));
        if (!it)
            continue;
        const auto bucket_id = it->getMapped();
        if (scratch.seen[bucket_id] == scratch.generation)
            continue;
        scratch.seen[bucket_id] = scratch.generation;
        const auto & bucket = route.buckets[bucket_id];
        for (size_t i = bucket.offset; i < bucket.offset + bucket.count; ++i)
        {
            if constexpr (count_work)
                ++work->term_references;
            const auto & ref = route.references[i];
            const auto result = markTermMatched(query, ref.clause, ref.term, states, matched_must_clauses);
            // has_must_not prevents accepting before a later prohibited
            // association in this same bucket (e.g. +apple -APPLE).
            if (result != ColumnMatchResult::Continue)
                return result;
        }
    }
    return ColumnMatchResult::Continue;
}

CompiledBooleanQuery compileBooleanQuery(
    const tipb::LocalMatchAgainstBooleanQuery & query,
    const LocalMatchAnalyzerConfig & config,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    const BuiltinStopwordLookup * stopwords = nullptr)
{
    CompiledBooleanQuery compiled;
    if (!config.use_ngram && (config.max_token_size == 0 || config.min_token_size > config.max_token_size))
        return {};
    auto analyze_query = [&](std::string_view text) {
        if (config.use_ngram)
            return analyzeNgramText(
                text,
                config.ngram_token_size,
                collator,
                stopwords,
                config.enable_stopword,
                NgramAnalysisMode::Query);
        return analyzeText(
            text,
            collator,
            stopwords,
            config.min_token_size,
            config.max_token_size,
            config.enable_stopword);
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
            throw Exception("Invalid Local MATCH Boolean node occurrence", ErrorCodes::ILLEGAL_COLUMN);
        }

        switch (node.term_type())
        {
        case tipb::LocalMatchAgainstBooleanTermType::LocalMatchAgainstBooleanTermWord:
        {
            const auto terms = analyze_query(node.text());
            clause.phrase = config.use_ngram;
            first_query_position = collectClauseTerms(clause, terms);
            break;
        }
        case tipb::LocalMatchAgainstBooleanTermType::LocalMatchAgainstBooleanTermPrefix:
        {
            if (config.use_ngram)
            {
                const auto terms = analyzeNgramText(
                    node.text(),
                    config.ngram_token_size,
                    collator,
                    stopwords,
                    config.enable_stopword,
                    NgramAnalysisMode::Prefix);
                if (!terms.empty())
                {
                    // TiDB expands an NGRAM prefix at least as long as one
                    // gram into a positional phrase of query ngrams. Matching
                    // only one analyzed term here makes prefixes such as
                    // "caf*" ("ca", "af" at token size 2) never match.
                    clause.phrase = true;
                    first_query_position = collectClauseTerms(clause, terms);
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
                    if (code_points < config.ngram_token_size)
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

            const auto terms = tokenizeText(node.text(), collator);
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
            if (code_points > config.max_token_size)
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
            first_query_position = collectClauseTerms(clause, terms);
            break;
        }
        default:
            throw Exception("Invalid Local MATCH Boolean node term type", ErrorCodes::ILLEGAL_COLUMN);
        }

        if (clause.phrase && !clause.terms.empty())
        {
            auto raw_terms = config.use_ngram
                ? analyzeNgramText(
                    node.text(),
                    config.ngram_token_size,
                    collator,
                    nullptr,
                    false,
                    NgramAnalysisMode::Query)
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
                collectClauseTerms(*clause.verification, raw_terms);
            }
        }

        if (clause.prefix && collator)
            clause.prefix_matcher = compilePrefixMatcher(clause.terms.front(), collator);

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
                return {};
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
        return {};

    return compiled;
}

size_t eligibleExactRouteTermCount(
    const CompiledBooleanQuery & query,
    const LocalMatchAnalyzerConfig & config,
    const TiDB::TiDBCollatorPtr & collator,
    LocalMatchAgainstTest::Matcher matcher,
    size_t min_terms,
    size_t budget)
{
    if (matcher == LocalMatchAgainstTest::Matcher::Linear || config.use_ngram || !collator)
        return {};
    // Compare and sortKey in these implementations encode the same weight
    // stream and use identical PAD/NO PAD preprocessing. GeneralCI emits one
    // 16-bit weight per rune; UCA emits the same expansion/ignorable-weight
    // stream consumed by compare. Binary keys preserve byte equality.
    using Type = TiDB::ITiDBCollator::CollatorType;
    switch (collator->getCollatorType())
    {
    case Type::UTF8_GENERAL_CI:
    case Type::UTF8MB4_GENERAL_CI:
    case Type::UTF8_UNICODE_CI:
    case Type::UTF8MB4_UNICODE_CI:
    case Type::UTF8MB4_0900_AI_CI:
    case Type::UTF8MB4_BIN:
    case Type::UTF8_BIN:
    case Type::UTF8MB4_0900_BIN:
    case Type::BINARY:
    case Type::LATIN1_BIN:
    case Type::ASCII_BIN:
        break;
    default:
        return {};
    }

    size_t terms = 0;
    // UCA may resize a document key buffer to 16 bytes per input byte;
    // allow string capacity growth independently of the query term sizes.
    size_t estimate = 32 * 1024 + 2 * 16 * exact_route_max_token_bytes;
    if (estimate > budget)
        return {};
    for (const auto & clause : query.clauses)
    {
        if (clause.phrase || clause.prefix || clause.verification)
            return {};
        for (const auto & term : clause.terms)
        {
            if (term.size() > exact_route_max_token_bytes)
                return {};
            // Conservative bound for power-of-two hash/POD capacities,
            // Arena growth and up to 16 key bytes per input byte. Charge
            // scratch growth too. Check before constructing any route.
            const size_t cost = 256 + 96 * term.size();
            if (cost > budget - estimate)
                return {};
            estimate += cost;
            ++terms;
        }
    }
    if (terms == 0 || (matcher == LocalMatchAgainstTest::Matcher::Automatic && terms < min_terms))
        return {};

    return terms;
}

std::unique_ptr<ExactTermRoute> buildExactTermRoute(
    const CompiledBooleanQuery & query,
    const TiDB::TiDBCollatorPtr & collator,
    size_t terms)
{
    auto route = std::make_unique<ExactTermRoute>();
    route->references.reserve(terms);
    route->buckets.reserve(terms);
    MatchKeyBuffer keys;
    for (size_t clause = 0; clause < query.clauses.size(); ++clause)
        for (size_t term = 0; term < query.clauses[clause].terms.size(); ++term)
        {
            const auto key = keys.key(query.clauses[clause].terms[term], collator);
            auto * it = route->dictionary.find(key);
            size_t bucket;
            if (it)
                bucket = it->getMapped();
            else
            {
                bucket = route->buckets.size();
                const StringRef owned(route->keys.insert(key.data, key.size), key.size);
                bool inserted;
                route->dictionary.emplace(owned, it, inserted);
                it->getMapped() = bucket;
                route->buckets.push_back(ExactTermRoute::Bucket{0, 0});
            }
            route->references.push_back(ExactTermRoute::Reference{bucket, clause, term});
        }
    std::sort(route->references.begin(), route->references.end(), [](const auto & a, const auto & b) {
        return a.bucket < b.bucket;
    });
    for (size_t i = 0; i < route->references.size(); ++i)
    {
        auto & bucket = route->buckets[route->references[i].bucket];
        if (bucket.count == 0)
            bucket.offset = i;
        ++bucket.count;
    }
    return route;
}

class FullTextColumnAccessor
{
public:
    explicit FullTextColumnAccessor(const IColumn & column)
    {
        const auto * constant = typeid_cast<const ColumnConst *>(&column);
        is_constant = constant != nullptr;
        const IColumn * data = constant ? &constant->getDataColumn() : &column;
        // ColumnNullable materializes constant nested data, so only an outer
        // ColumnConst needs to map both the null map and the string to row 0.
        nullable_column = typeid_cast<const ColumnNullable *>(data);
        if (nullable_column)
            data = &nullable_column->getNestedColumn();
        string_column = checkAndGetColumn<ColumnString>(data);
        if (!string_column)
            throw Exception("Full-text arguments must be string columns", ErrorCodes::ILLEGAL_COLUMN);
    }

    bool isNull(size_t row) const
    {
        return nullable_column && nullable_column->getNullMapData()[is_constant ? 0 : row] != 0;
    }

    std::string_view getString(size_t row) const
    {
        const auto value = string_column->getDataAt(is_constant ? 0 : row);
        return {value.data, value.size};
    }

private:
    const ColumnString * string_column;
    const ColumnNullable * nullable_column;
    bool is_constant;
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
    // Validate every node before NULL-search or analyzer no-match shortcuts.
    // Missing/unknown proto2 enum values read as Invalid, not as valid clauses.
    for (const auto & node : query.nodes())
    {
        if (node.occur() != tipb::LocalMatchAgainstBooleanOccurMust
            && node.occur() != tipb::LocalMatchAgainstBooleanOccurMustNot
            && node.occur() != tipb::LocalMatchAgainstBooleanOccurShould)
            return false;
        if (node.term_type() != tipb::LocalMatchAgainstBooleanTermWord
            && node.term_type() != tipb::LocalMatchAgainstBooleanTermPrefix
            && node.term_type() != tipb::LocalMatchAgainstBooleanTermPhrase)
            return false;
    }
    return true;
}

class FunctionLocalMatchAgainstBoolean final : public IFunction
{
public:
    explicit FunctionLocalMatchAgainstBoolean(
        LocalMatchAgainstTest::Matcher matcher_ = LocalMatchAgainstTest::Matcher::Automatic,
        LocalMatchAgainstTest::Work * work_ = nullptr,
        size_t budget_ = exact_route_budget,
        UInt32 generation_ = 0)
        : matcher(matcher_)
        , work(work_)
        , budget(budget_)
        , generation(generation_)
    {}
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
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & argument = arguments[i];
            const auto nested_type = removeNullable(argument);
            // An untyped SQL NULL search arrives as Nullable(Nothing).
            // Documents and metadata still require string types.
            const bool null_query = i == 0 && nested_type->getTypeId() == TypeIndex::Nothing;
            if (!nested_type->isString() && !null_query)
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
        if (work)
            executeInternal<true>(block, arguments, result);
        else
            executeInternal<false>(block, arguments, result);
    }

    template <bool count_work>
    void executeInternal(Block & block, const ColumnNumbers & arguments, size_t result) const
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
        const auto finish_result = [&] {
            ColumnPtr result_column = std::move(output);
            if (null_map)
                result_column = ColumnNullable::create(std::move(result_column), std::move(null_map));
            block.getByPosition(result).column = std::move(result_column);
        };

        tipb::LocalMatchAgainstBooleanQuery protocol_boolean_query;
        if (arguments.size() <= 2
            || !decodeLocalMatchAgainstBooleanQuery(
                *block.getByPosition(arguments.back()).column,
                protocol_boolean_query))
            throw Exception(
                "local_match_against_boolean requires valid Boolean query metadata",
                ErrorCodes::ILLEGAL_COLUMN);
        if (query_column->isNullAt(0))
        {
            // InnoDB interprets NULL AGAINST as an empty search: non-NULL zero.
            finish_result();
            return;
        }
        const size_t document_argument_end = arguments.size() - 1;
        const LocalMatchAnalyzerConfig config(protocol_boolean_query);
        // Keep scalar values in the row hot path. Passing the config by
        // reference changes Release code generation, notably with stopwords
        // disabled; the config object belongs to query compilation only.
        const bool use_ngram = config.use_ngram;
        const size_t ngram_token_size = config.ngram_token_size;
        const size_t min_token_size = config.min_token_size;
        const size_t max_token_size = config.max_token_size;
        const bool enable_stopword = config.enable_stopword;
        const auto * const stopword_collator
            = enable_stopword ? TiDB::ITiDBCollator::getCollator(protocol_boolean_query.stopword_collation()) : nullptr;
        if (enable_stopword && !stopword_collator)
            throw Exception(
                "local_match_against_boolean requires a supported stopword collation when stopwords are enabled",
                ErrorCodes::ILLEGAL_COLUMN);
        std::optional<BuiltinStopwordLookup> stopword_lookup;
        if (enable_stopword)
            stopword_lookup.emplace(stopword_collator);
        const auto * stopwords = stopword_lookup ? &*stopword_lookup : nullptr;
        CompiledBooleanQuery compiled_query = compileBooleanQuery(protocol_boolean_query, config, collator, stopwords);
        if (compiled_query.clauses.empty())
        {
            finish_result();
            return;
        }
        std::vector<FullTextColumnAccessor> document_columns;
        document_columns.reserve(document_argument_end - 1);
        for (size_t arg = 1; arg < document_argument_end; ++arg)
            document_columns.emplace_back(*block.getByPosition(arguments[arg]).column);
        const bool large_block = rows >= exact_route_large_block_min_rows;
        const size_t min_route_terms = large_block ? exact_route_min_terms_large_block : exact_route_min_terms;
        const size_t route_terms
            = eligibleExactRouteTermCount(compiled_query, config, collator, matcher, min_route_terms, budget);
        const size_t min_route_tokens = large_block ? exact_route_min_tokens_large_block : exact_route_min_tokens;
        bool may_use_index = matcher == LocalMatchAgainstTest::Matcher::Indexed;
        if (route_terms && !may_use_index)
        {
            // A STANDARD token consumes at least one raw byte, and consecutive
            // tokens need a separator. This cheap lower bound bypasses the
            // routing row loop for wholly short blocks without tokenizing twice.
            // Use one byte even for unusual analyzer bounds to avoid overflow.
            const size_t min_document_bytes = min_route_tokens * 2 - 1;
            for (const auto & column : document_columns)
            {
                for (size_t row = 0; row < rows; ++row)
                    if (!column.isNull(row) && column.getString(row).size() >= min_document_bytes)
                    {
                        may_use_index = true;
                        break;
                    }
                if (may_use_index)
                    break;
            }
        }
        // Compile separate row paths so ineligible queries discard optional
        // scratch and routing branches from their analyzer/matcher hot loop.
        const auto execute_rows = [&,
                                   use_ngram,
                                   ngram_token_size,
                                   min_token_size,
                                   max_token_size,
                                   enable_stopword]<bool enable_index>() ALWAYS_INLINE {
            std::vector<ClauseMatchState> clause_states;
            initializeClauseMatchStates(compiled_query.clauses, clause_states);
            FullTextColumn analyzed_column;
            FullTextColumn unfiltered_column;
            AnalyzerScratch analyzer_scratch;

            std::optional<ExactMatchScratch> exact_scratch;
            bool route_attempted = false;
            for (size_t row = 0; row < rows; ++row)
            {
                if constexpr (enable_index)
                    if (exact_scratch)
                        exact_scratch->nextRow();
                resetClauseMatchStates(clause_states);
                size_t matched_must_clauses = 0;
                auto row_result = ColumnMatchResult::Continue;
                for (const auto & document_column : document_columns)
                {
                    // A NULL MATCH column contributes no tokens. This is
                    // intentional: #70485 relies on a row with a NULL body
                    // still matching a required term from another MATCH column.
                    if (document_column.isNull(row))
                        continue;
                    const auto document = document_column.getString(row);
                    analyzeColumnInto(
                        document,
                        analyzed_column,
                        analyzer_scratch,
                        use_ngram,
                        ngram_token_size,
                        collator,
                        stopwords,
                        min_token_size,
                        max_token_size,
                        enable_stopword);
                    if constexpr (enable_index)
                    {
                        // Do not allocate a query dictionary for blocks containing
                        // only short documents. Build at most once, after metadata
                        // validation and complete clause compilation. Earlier short
                        // columns may already have contributed to this row's state.
                        if (!route_attempted
                            && (matcher == LocalMatchAgainstTest::Matcher::Indexed
                                || analyzed_column.size() >= min_route_tokens))
                        {
                            route_attempted = true;
                            compiled_query.exact_route = buildExactTermRoute(compiled_query, collator, route_terms);
                            if (compiled_query.exact_route)
                            {
                                exact_scratch.emplace(compiled_query.exact_route->buckets.size(), generation);
                                exact_scratch->nextRow();
                                if constexpr (count_work)
                                    ++work->route_builds;
                            }
                        }
                    }
                    // A filtered phrase hit is the first point that needs the
                    // raw tokens. Share the stream across clauses in this column.
                    bool unfiltered_ready = false;
                    const auto get_unfiltered_column = [&]() -> const FullTextColumn & {
                        if (!unfiltered_ready)
                        {
                            analyzeColumnInto(
                                document,
                                unfiltered_column,
                                analyzer_scratch,
                                use_ngram,
                                ngram_token_size,
                                collator,
                                nullptr,
                                0,
                                std::numeric_limits<size_t>::max(),
                                false);
                            unfiltered_ready = true;
                            if constexpr (count_work)
                                ++work->unfiltered_column_analyses;
                        }
                        return unfiltered_column;
                    };
                    if constexpr (enable_index)
                    {
                        // STANDARD only emits valid UTF-8 BMP word tokens: malformed
                        // bytes become RuneError separators. Do not re-interpret the
                        // raw document. An oversized analyzed token falls back for
                        // this whole column before any state is modified.
                        const bool indexed = exact_scratch
                            && (matcher == LocalMatchAgainstTest::Matcher::Indexed
                                || analyzed_column.size() >= min_route_tokens)
                            // Valid BMP tokens use at most three bytes per code
                            // point. The usual analyzer bound proves this without
                            // another column pass, including early-hit OR queries.
                            && (max_token_size <= exact_route_max_token_bytes / 3
                                || std::all_of(analyzed_column.begin(), analyzed_column.end(), [](const auto & token) {
                                       return token.value().size() <= exact_route_max_token_bytes;
                                   }));
                        if constexpr (count_work)
                        {
                            if (indexed)
                                ++work->indexed_columns;
                            else
                                ++work->linear_columns;
                        }
                        row_result = indexed ? updateExactMatchStatesForColumn<count_work>(
                                         compiled_query,
                                         analyzed_column,
                                         collator,
                                         clause_states,
                                         matched_must_clauses,
                                         *exact_scratch,
                                         work)
                                             : updateBooleanMatchStatesForColumn(
                                                 compiled_query,
                                                 analyzed_column,
                                                 get_unfiltered_column,
                                                 collator,
                                                 clause_states,
                                                 matched_must_clauses);
                    }
                    else
                    {
                        if constexpr (count_work)
                            ++work->linear_columns;
                        row_result = updateBooleanMatchStatesForColumn(
                            compiled_query,
                            analyzed_column,
                            get_unfiltered_column,
                            collator,
                            clause_states,
                            matched_must_clauses);
                    }
                    if (row_result != ColumnMatchResult::Continue)
                        break;
                }
                if (row_result != ColumnMatchResult::Continue)
                    output_data[row] = row_result == ColumnMatchResult::Accepted;
                else if (compiled_query.must_clause_count != 0)
                    output_data[row] = matched_must_clauses == compiled_query.must_clause_count;
                else
                {
                    // A matched MUST NOT rejects immediately. With no MUST
                    // clauses, any recorded match is therefore a SHOULD hit.
                    output_data[row] = compiled_query.has_must_not
                        && std::any_of(clause_states.begin(), clause_states.end(), [](const auto & state) {
                                           return state.matched;
                                       });
                }
            }
        };
        if (route_terms && may_use_index)
            execute_rows.template operator()<true>();
        else
            execute_rows.template operator()<false>();
        finish_result();
    }

private:
    TiDB::TiDBCollatorPtr collator = nullptr;
    LocalMatchAgainstTest::Matcher matcher;
    LocalMatchAgainstTest::Work * work;
    size_t budget;
    UInt32 generation;
};
} // namespace

FunctionPtr LocalMatchAgainstTest::create(Matcher matcher, Work & work, size_t budget, UInt32 generation)
{
    return std::make_shared<FunctionLocalMatchAgainstBoolean>(matcher, &work, budget, generation);
}

void registerFunctionsLocalMatchAgainst(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLocalMatchAgainstBoolean>(FunctionFactory::CaseInsensitive);
}
} // namespace DB

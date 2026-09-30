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
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsFullText.h>
#include <Poco/Unicode.h>
#include <Poco/UTF8String.h>
#include <TiDB/Collation/Collator.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <memory>
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
constexpr std::string_view ngram_parser = "NGRAM_V1";

struct FullTextToken
{
    String text;
    size_t position = 0;
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
};

using FullTextColumn = std::vector<FullTextToken>;
using FullTextDocument = std::vector<FullTextColumn>;

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

std::vector<FullTextToken> tokenizeText(std::string_view text, const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    std::vector<FullTextToken> result;
    size_t position = 0;
    for (size_t i = 0; i < text.size();)
    {
        const auto [code_point, length] = decodeCodePoint(text, i);
        if (!isFullTextToken(code_point))
        {
            i += length;
            continue;
        }

        String token;
        while (i < text.size())
        {
            const auto [code_point, length] = decodeCodePoint(text, i);
            if (!isFullTextToken(code_point))
                break;
            token.append(text.data() + i, length);
            i += length;
        }

        // If a collation is present, retain the source spelling and let the
        // collator decide case and accent equivalence during matching. This
        // is important for binary collations, where lower-casing would make
        // a case-sensitive MATCH unexpectedly case-insensitive. Keep the
        // legacy lower-case behavior for callers without collation metadata.
        if (!collator)
            token = Poco::UTF8::toLower(token);
        result.push_back({std::move(token), position++});
    }
    return result;
}

bool isDefaultStopword(const String & token, const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    static const std::unordered_set<String> stopwords{
        "a",    "about", "an",   "are",  "as",   "at",   "be",   "by",   "com", "de", "en", "for",
        "from", "how",   "i",    "in",    "is",    "it",   "la",   "of",   "on",   "or",   "that", "the",
        "this", "to",    "was",  "what",  "when",  "where", "who",  "will", "with", "und", "www"};
    // Stopwords are stored in canonical lower-case form. Keep their existing
    // case-insensitive behavior independently from document matching.
    if (stopwords.contains(Poco::UTF8::toLower(token)))
        return true;
    if (collator)
    {
        return std::any_of(stopwords.begin(), stopwords.end(), [&](const String & stopword) {
            return collator->compare(token.data(), token.size(), stopword.data(), stopword.size()) == 0;
        });
    }
    return false;
}

std::vector<FullTextToken> analyzeText(
    std::string_view text,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    std::vector<FullTextToken> result;
    for (auto & token : tokenizeText(text, collator))
    {
        const auto code_points = UTF8::countCodePoints(
            reinterpret_cast<const UInt8 *>(token.text.data()),
            token.text.size());
        if (code_points >= min_token_size && code_points <= max_token_size
            && (!enable_stopword || !isDefaultStopword(token.text, collator)))
            result.push_back(std::move(token));
    }
    return result;
}

std::vector<FullTextToken> analyzeNgramText(
    std::string_view text,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    if (ngram_token_size == 0)
        return {};

    std::vector<FullTextToken> result;
    size_t next_position_base = 0;
    for (auto & token : tokenizeText(text, collator))
    {
        std::vector<size_t> char_boundaries;
        char_boundaries.reserve(token.text.size() + 1);
        char_boundaries.push_back(0);
        for (size_t offset = 0; offset < token.text.size();)
        {
            const auto [code_point, length] = decodeCodePoint(token.text, offset);
            (void)code_point;
            offset += length;
            char_boundaries.push_back(offset);
        }

        const size_t char_count = char_boundaries.size() - 1;
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
            result.push_back({token.text.substr(begin, end - begin), base_position + start});
        }
        next_position_base = base_position + char_count - ngram_token_size + 1;
    }
    return result;
}

bool textEquals(std::string_view lhs, std::string_view rhs, const TiDB::TiDBCollatorPtr & collator)
{
    if (!collator)
        return lhs == rhs;
    return collator->compare(lhs.data(), lhs.size(), rhs.data(), rhs.size()) == 0;
}

bool textStartsWith(std::string_view value, std::string_view prefix, const TiDB::TiDBCollatorPtr & collator)
{
    if (!collator)
        return value.starts_with(prefix);

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
    auto matcher = collator->pattern();
    matcher->compile(pattern, '\\');
    return matcher->match(value.data(), value.size());
}

bool isBooleanWhitespace(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

bool parseBooleanQuery(
    std::string_view query,
    std::vector<BooleanClause> & clauses,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    for (size_t i = 0; i < query.size();)
    {
        while (i < query.size() && isBooleanWhitespace(query[i]))
            ++i;
        if (i == query.size())
            break;

        BooleanClause clause;
        // The STANDARD parser used by #70485 accepts only the operators that
        // affect filtering. Other InnoDB operators affect scoring or phrase
        // proximity and must not be silently treated as ordinary text.
        if (query[i] == '%' || query[i] == '(' || query[i] == ')' || query[i] == '<' || query[i] == '>'
            || query[i] == '~' || query[i] == '@')
            return false;

        if (query[i] == '+' || query[i] == '-')
        {
            clause.modifier = query[i] == '+' ? BooleanClause::Modifier::Must : BooleanClause::Modifier::MustNot;
            ++i;
        }

        if (i == query.size())
            return false;

        // InnoDB accepts a leading wildcard as a no-op. A trailing wildcard
        // is handled below and turns a term into a prefix query.
        if (query[i] == '*')
        {
            ++i;
            if (i == query.size())
                continue;
        }

        String raw;
        if (query[i] == '"')
        {
            clause.phrase = true;
            ++i;
            const size_t start = i;
            while (i < query.size() && query[i] != '"' && query[i] != '\n')
                ++i;
            if (i == query.size() || query[i] != '"')
                return false;
            raw.assign(query.substr(start, i - start));
            ++i;
        }
        else
        {
            const size_t start = i;
            while (i < query.size() && !isBooleanWhitespace(query[i]) && query[i] != '+' && query[i] != '-'
                   && query[i] != '*' && query[i] != '%' && query[i] != '(' && query[i] != ')' && query[i] != '<'
                   && query[i] != '>' && query[i] != '~' && query[i] != '@')
                ++i;
            if (start == i)
                return false;
            raw.assign(query.substr(start, i - start));
            // The scan above stops before `*`, so consume the wildcard here
            // and attach prefix semantics to this term. Leaving it for the
            // next iteration incorrectly turns `run*` into an exact `run`
            // match (and makes it fail against `runner`).
            if (i < query.size() && query[i] == '*')
            {
                clause.prefix = true;
                ++i;
            }
        }

        const auto terms = clause.prefix ? tokenizeText(raw, collator) : analyzeText(raw, collator);
        if (clause.prefix && terms.size() != 1)
            continue;

        const size_t first_position = terms.empty() ? 0 : terms.front().position;
        for (const auto & term : terms)
        {
            const auto code_points = UTF8::countCodePoints(
                reinterpret_cast<const UInt8 *>(term.text.data()),
                term.text.size());
            if (code_points <= default_max_token_size && (clause.prefix || code_points >= default_min_token_size)
                && (clause.prefix || !isDefaultStopword(term.text)))
            {
                clause.terms.push_back(term.text);
                if (clause.phrase)
                    clause.offsets.push_back(term.position - first_position);
            }
        }
        if (!clause.terms.empty() || clause.modifier == BooleanClause::Modifier::Must)
            clauses.push_back(std::move(clause));
    }
    return true;
}

bool matchesPhraseInColumn(
    const BooleanClause & clause,
    const FullTextColumn & document,
    const TiDB::TiDBCollatorPtr & collator)
{
    if (clause.terms.empty() || clause.terms.size() != clause.offsets.size())
        return false;

    for (const auto & start : document)
    {
        bool matched = true;
        for (size_t i = 0; i < clause.terms.size(); ++i)
        {
            const auto expected_position = start.position + clause.offsets[i];
            const auto it = std::find_if(document.begin(), document.end(), [&](const FullTextToken & token) {
                return token.position == expected_position;
            });
            if (it == document.end() || !textEquals(it->text, clause.terms[i], collator))
            {
                matched = false;
                break;
            }
        }
        if (matched)
            return true;
    }
    return false;
}

bool matchesClause(
    const BooleanClause & clause,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator)
{
    if (clause.terms.empty())
        return false;

    if (clause.phrase)
        return std::any_of(document.begin(), document.end(), [&](const FullTextColumn & column) {
            return matchesPhraseInColumn(clause, column, collator);
        });

    return std::all_of(clause.terms.begin(), clause.terms.end(), [&](const String & term) {
        return std::any_of(document.begin(), document.end(), [&](const FullTextColumn & column) {
            return std::any_of(column.begin(), column.end(), [&](const FullTextToken & token) {
                return clause.prefix ? textStartsWith(token.text, term, collator) : textEquals(token.text, term, collator);
            });
        });
    });
}

size_t countClauseMatches(
    const BooleanClause & clause,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator)
{
    if (!matchesClause(clause, document, collator))
        return 0;

    if (!clause.phrase)
    {
        size_t count = 0;
        for (const auto & term : clause.terms)
        {
            for (const auto & column : document)
            {
                count += std::count_if(column.begin(), column.end(), [&](const FullTextToken & token) {
                    return clause.prefix ? textStartsWith(token.text, term, collator) : textEquals(token.text, term, collator);
                });
            }
        }
        return count;
    }

    size_t count = 0;
    for (const auto & column : document)
    {
        for (const auto & start : column)
        {
            bool matched = true;
            for (size_t i = 0; i < clause.terms.size(); ++i)
            {
                const auto expected_position = start.position + clause.offsets[i];
                const auto it = std::find_if(column.begin(), column.end(), [&](const FullTextToken & token) {
                    return token.position == expected_position;
                });
                if (it == column.end() || !textEquals(it->text, clause.terms[i], collator))
                {
                    matched = false;
                    break;
                }
            }
            if (matched)
                ++count;
        }
    }
    return count;
}

FullTextColumn analyzeColumn(std::string_view document, const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    return analyzeText(document, collator);
}

FullTextColumn analyzeColumn(
    std::string_view document,
    bool use_ngram,
    size_t ngram_token_size,
    const TiDB::TiDBCollatorPtr & collator = nullptr,
    size_t min_token_size = default_min_token_size,
    size_t max_token_size = default_max_token_size,
    bool enable_stopword = true)
{
    return use_ngram ? analyzeNgramText(document, ngram_token_size, collator)
                     : analyzeText(document, collator, min_token_size, max_token_size, enable_stopword);
}

Float64 matchBooleanScore(
    const std::vector<BooleanClause> & clauses,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
	if (clauses.empty())
		return 0;

	bool has_positive = false;
	bool has_must = false;
	bool positive_match = false;
	Float64 score = 0;
	for (const auto & clause : clauses)
	{
        const bool matched = matchesClause(clause, document, collator);
        const auto clause_score = static_cast<Float64>(countClauseMatches(clause, document, collator));
		switch (clause.modifier)
		{
		case BooleanClause::Modifier::Must:
			has_must = true;
			if (!matched)
				return 0;
			score += clause_score;
			break;
		case BooleanClause::Modifier::MustNot:
			if (matched)
				return 0;
			break;
		case BooleanClause::Modifier::Should:
			has_positive = true;
			positive_match = positive_match || matched;
			score += clause_score;
			break;
		}
	}

	// With a required term, unprefixed terms are optional. Without one, at
	// least one unprefixed term must match, matching BOOLEAN MODE semantics.
	if (!(has_must || !has_positive || positive_match))
		return 0;
	// A query containing only prohibited terms has no positive term to score,
	// but an accepted row still has to pass the boolean filter.
    return score > 0 ? score : 1;
}

bool matchBooleanPredicate(
    const std::vector<BooleanClause> & clauses,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    if (clauses.empty())
        return false;

    bool has_positive = false;
    bool has_must = false;
    bool positive_match = false;
    for (const auto & clause : clauses)
    {
        const bool matched = matchesClause(clause, document, collator);
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

Float64 matchBooleanScore(
    std::string_view query,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    std::vector<BooleanClause> clauses;
    if (!parseBooleanQuery(query, clauses, collator))
        return 0;
    return matchBooleanScore(clauses, document, collator);
}

Float64 matchBooleanScore(
    const tipb::FTSBooleanQuery & query,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
	std::vector<BooleanClause> clauses;
	const bool use_ngram = query.query_tokenizer() == ngram_parser;
	const size_t ngram_token_size = query.ngram_token_size() == 0 ? default_ngram_token_size : query.ngram_token_size();
	const bool has_standard_config = query.innodb_ft_min_token_size() != 0 || query.innodb_ft_max_token_size() != 0;
	const size_t min_token_size = has_standard_config ? query.innodb_ft_min_token_size() : default_min_token_size;
	const size_t max_token_size = has_standard_config ? query.innodb_ft_max_token_size() : default_max_token_size;
	const bool enable_stopword = has_standard_config ? query.innodb_ft_enable_stopword() : true;
	if (!use_ngram && (max_token_size == 0 || min_token_size > max_token_size))
		return 0;
	auto analyze_query = [&](std::string_view text) {
		return use_ngram ? analyzeNgramText(text, ngram_token_size, collator)
					 : analyzeText(text, collator, min_token_size, max_token_size, enable_stopword);
	};
	for (const auto & node : query.nodes())
	{
		if (!node.has_term())
			return 0;

		const auto & term = node.term();
		BooleanClause clause;
		switch (node.occur())
		{
		case tipb::FTSBooleanOccur::FTSBooleanOccurMust:
			clause.modifier = BooleanClause::Modifier::Must;
			break;
		case tipb::FTSBooleanOccur::FTSBooleanOccurMustNot:
			clause.modifier = BooleanClause::Modifier::MustNot;
			break;
		case tipb::FTSBooleanOccur::FTSBooleanOccurShould:
			clause.modifier = BooleanClause::Modifier::Should;
			break;
		default:
			return 0;
		}

		switch (term.term_type())
		{
		case tipb::FTSBooleanTermType::FTSBooleanTermWord:
		{
			const auto terms = analyze_query(term.text());
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
		case tipb::FTSBooleanTermType::FTSBooleanTermPrefix:
		{
			const auto terms = use_ngram ? analyzeNgramText(term.text(), ngram_token_size, collator)
							 : tokenizeText(term.text(), collator);
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
				const auto source_terms = tokenizeText(term.text(), collator);
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
					clauses.push_back(std::move(clause));
				continue;
			}

			clause.prefix = true;
			if (terms.size() != 1)
			{
				if (clause.modifier == BooleanClause::Modifier::Must)
					clauses.push_back(std::move(clause));
				continue;
			}
			const auto code_points = UTF8::countCodePoints(
				reinterpret_cast<const UInt8 *>(terms.front().text.data()),
				terms.front().text.size());
			if (code_points > max_token_size)
			{
				if (clause.modifier == BooleanClause::Modifier::Must)
					clauses.push_back(std::move(clause));
				continue;
			}
			clause.terms.push_back(terms.front().text);
			break;
		}
		case tipb::FTSBooleanTermType::FTSBooleanTermPhrase:
		{
			clause.phrase = true;
			const auto terms = analyze_query(term.text());
			const size_t first_position = terms.empty() ? 0 : terms.front().position;
			for (const auto & token : terms)
			{
				clause.terms.push_back(token.text);
				clause.offsets.push_back(token.position - first_position);
			}
			break;
		}
		default:
			return 0;
		}

		if (!clause.terms.empty() || clause.modifier == BooleanClause::Modifier::Must)
			clauses.push_back(std::move(clause));
	}

	// A BOOLEAN MODE query containing only prohibited terms has no positive
	// branch. TiDB's local evaluator treats it as matching no rows.
	if (std::none_of(clauses.begin(), clauses.end(), [](const BooleanClause & clause) {
			return clause.modifier != BooleanClause::Modifier::MustNot;
		}))
		return 0;

	// The protocol path is the no-score MATCH ... AGAINST BOOLEAN MODE
	// predicate introduced by #70484/#70485. TiDB's local evaluator returns
	// a boolean 0/1 result, so do not expose term-frequency counts here.
	return matchBooleanPredicate(clauses, document, collator) ? 1 : 0;
}

Float64 matchBooleanScore(
    std::string_view query,
    std::string_view document,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    return matchBooleanScore(query, FullTextDocument{analyzeColumn(document, collator)}, collator);
}

size_t countTermMatches(
    const String & term,
    const FullTextColumn & document,
    const TiDB::TiDBCollatorPtr & collator)
{
    return std::count_if(document.begin(), document.end(), [&](const FullTextToken & token) {
        return textEquals(token.text, term, collator);
    });
}

Float64 matchNaturalLanguageScore(
    std::string_view query,
    const FullTextDocument & document,
    const TiDB::TiDBCollatorPtr & collator = nullptr)
{
    const auto query_tokens = analyzeText(query, collator);
    if (query_tokens.empty())
        return 0;

    std::unordered_set<String> unique_query_tokens;
    Float64 score = 0;
    for (const auto & query_token : query_tokens)
    {
        if (!unique_query_tokens.insert(query_token.text).second)
            continue;
        for (const auto & column : document)
            score += static_cast<Float64>(countTermMatches(query_token.text, column, collator));
    }
    return score;
}

bool queryUsesBooleanSyntax(std::string_view query)
{
    return query.find_first_of("+-\"*%()<>~@") != std::string_view::npos;
}

String getStringAt(const IColumn & column, size_t row)
{
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (column_nullable->getNullMapData()[row])
            return {};
        return getStringAt(column_nullable->getNestedColumn(), row);
    }

    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
        return column_const->getValue<String>();

    const auto * column_string = checkAndGetColumn<ColumnString>(&column);
    if (column_string == nullptr)
        throw Exception("Full-text arguments must be string columns", ErrorCodes::ILLEGAL_COLUMN);

    const auto & chars = column_string->getChars();
    const auto & offsets = column_string->getOffsets();
    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t end = offsets[row];
    return String(reinterpret_cast<const char *>(&chars[begin]), end - begin - 1);
}

bool isNullAt(const IColumn & column, size_t row)
{
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
        return column_nullable->getNullMapData()[row];
    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
    {
        if (const auto * nested_nullable = typeid_cast<const ColumnNullable *>(&column_const->getDataColumn()))
            return nested_nullable->getNullMapData()[0];
    }
    return false;
}

bool decodeFTSBooleanQuery(const IColumn & column, tipb::FTSBooleanQuery & query)
{
    const auto * constant = typeid_cast<const ColumnConst *>(&column);
    if (constant == nullptr)
        return false;
    const auto encoded = constant->getValue<String>();
    return query.ParseFromString(encoded);
}

class FunctionFTSMatchWord final : public IFunction
{
public:
    static constexpr auto name = "fts_match_word";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFTSMatchWord>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
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
                "The query argument of fts_match_word must be constant",
                ErrorCodes::ILLEGAL_COLUMN);

        const auto & document_column = block.getByPosition(arguments[1]).column;
        auto output = ColumnFloat64::create(document_column->size());
        auto & output_data = output->getData();

        if (isNullAt(*query_column, 0))
        {
            auto null_map = ColumnUInt8::create(document_column->size(), 1);
            block.getByPosition(result).column = ColumnNullable::create(std::move(output), std::move(null_map));
            return;
        }
        const auto query = query_column->getValue<String>();

        if (block.getByPosition(arguments[1]).type->isNullable() || document_column->isColumnNullable())
        {
            auto null_map = ColumnUInt8::create(document_column->size(), 0);
            auto & null_map_data = null_map->getData();
            for (size_t row = 0; row < document_column->size(); ++row)
            {
                if (isNullAt(*document_column, row))
                    null_map_data[row] = 1;
                else
                    output_data[row] = matchBooleanScore(query, getStringAt(*document_column, row), collator);
            }
            block.getByPosition(result).column = ColumnNullable::create(std::move(output), std::move(null_map));
            return;
        }

        if (const auto * document = checkAndGetColumn<ColumnString>(&*document_column))
        {
            const auto & chars = document->getChars();
            const auto & offsets = document->getOffsets();
            for (size_t row = 0, begin = 0; row < offsets.size(); ++row)
            {
                const size_t end = offsets[row];
                const size_t length = end - begin - 1;
                output_data[row]
                    = matchBooleanScore(
                        query,
                        std::string_view(reinterpret_cast<const char *>(&chars[begin]), length),
                        collator);
                begin = end;
            }
        }
        else if (const auto * document = typeid_cast<const ColumnConst *>(&*document_column))
        {
            const auto value = document->getValue<String>();
            const Float64 matched = matchBooleanScore(query, value, collator);
            std::fill(output_data.begin(), output_data.end(), matched);
        }
        else
        {
            throw Exception(
                "Illegal column " + document_column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
        block.getByPosition(result).column = std::move(output);
    }

private:
    TiDB::TiDBCollatorPtr collator;
};

class FunctionFTSMatchExpression : public IFunction
{
public:
    static constexpr auto name = "fts_match_expression";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFTSMatchExpression>(); }

    explicit FunctionFTSMatchExpression(bool require_boolean_query = false)
        : require_boolean_query(require_boolean_query)
    {}

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
        if (arguments.size() < 2)
            throw Exception("fts_match_expression requires a query and at least one column");
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
                "The query argument of fts_match_expression must be constant",
                ErrorCodes::ILLEGAL_COLUMN);

        const size_t rows = block.getByPosition(arguments[1]).column->size();
        auto output = ColumnFloat64::create(rows, 0);
        auto & output_data = output->getData();
        const bool nullable = block.getByPosition(result).type->isNullable();
        auto null_map = nullable ? ColumnUInt8::create(rows, 0) : nullptr;
        auto * null_map_data = null_map ? &null_map->getData() : nullptr;

        if (isNullAt(*query_column, 0))
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
        const auto query = query_column->getValue<String>();
        size_t document_argument_end = arguments.size();
        tipb::FTSBooleanQuery protocol_boolean_query;
        const bool has_protocol_boolean_query = require_boolean_query;
        if (require_boolean_query
            && (arguments.size() <= 2
                || !decodeFTSBooleanQuery(*block.getByPosition(arguments.back()).column, protocol_boolean_query)))
            throw Exception(
                "fts_match_expression_with_boolean_query requires valid Boolean query metadata",
                ErrorCodes::ILLEGAL_COLUMN);
        if (has_protocol_boolean_query)
            --document_argument_end;
        const bool use_ngram = has_protocol_boolean_query && protocol_boolean_query.query_tokenizer() == ngram_parser;
        const size_t ngram_token_size = !use_ngram || protocol_boolean_query.ngram_token_size() == 0
            ? default_ngram_token_size
            : protocol_boolean_query.ngram_token_size();
        const bool has_standard_config = has_protocol_boolean_query
            && (protocol_boolean_query.innodb_ft_min_token_size() != 0
                || protocol_boolean_query.innodb_ft_max_token_size() != 0);
        const size_t min_token_size = has_standard_config
            ? protocol_boolean_query.innodb_ft_min_token_size()
            : default_min_token_size;
        const size_t max_token_size = has_standard_config
            ? protocol_boolean_query.innodb_ft_max_token_size()
            : default_max_token_size;
        const bool enable_stopword = !has_standard_config || protocol_boolean_query.innodb_ft_enable_stopword();

        for (size_t row = 0; row < rows; ++row)
        {
            FullTextDocument document;
            document.reserve(document_argument_end - 1);
            for (size_t arg = 1; arg < document_argument_end; ++arg)
            {
                // A NULL MATCH column contributes no tokens. This is
                // intentional: #70485 relies on a row with a NULL body
                // still matching a required term from another MATCH column.
                if (isNullAt(*block.getByPosition(arguments[arg]).column, row))
                {
                    document.emplace_back();
                    continue;
                }
                document.push_back(
                    analyzeColumn(
                        getStringAt(*block.getByPosition(arguments[arg]).column, row),
                        use_ngram,
                        ngram_token_size,
                        collator,
                        min_token_size,
                        max_token_size,
                        enable_stopword));
            }
            if (has_protocol_boolean_query)
                output_data[row] = matchBooleanScore(protocol_boolean_query, document, collator);
            else if (queryUsesBooleanSyntax(query))
                output_data[row] = matchBooleanScore(query, document, collator);
            else
                output_data[row] = matchNaturalLanguageScore(query, document, collator);
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
    bool require_boolean_query;
};

class FunctionFTSMatchExpressionWithBooleanQuery final : public FunctionFTSMatchExpression
{
public:
    static constexpr auto name = "fts_match_expression_with_boolean_query";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFTSMatchExpressionWithBooleanQuery>(); }

    FunctionFTSMatchExpressionWithBooleanQuery()
        : FunctionFTSMatchExpression(true)
    {}

    String getName() const override { return name; }
};
}

void registerFunctionsFullText(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFTSMatchWord>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionFTSMatchExpression>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionFTSMatchExpressionWithBooleanQuery>(FunctionFactory::CaseInsensitive);
}
} // namespace DB

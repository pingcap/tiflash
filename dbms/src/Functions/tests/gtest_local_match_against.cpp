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

#include <Common/UTF8Helpers.h>
#include <Functions/LocalMatchAgainstTokenChars.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Collation/Collator.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <array>

namespace DB::tests
{
class TestLocalMatchAgainst : public DB::tests::FunctionTest
{
};

namespace
{
// Count token comparisons rather than wall time, so a repeated full-column
// verification regression is deterministic even on a loaded CI host.
class CountingMatchCollator final : public TiDB::ITiDBCollator
{
public:
    CountingMatchCollator()
        : ITiDBCollator(UTF8MB4_BIN)
        , inner(getCollator(UTF8MB4_BIN))
    {
        collator_type = inner->getCollatorType();
    }

    int compare(const char * lhs, size_t lhs_size, const char * rhs, size_t rhs_size) const override
    {
        ++comparisons;
        return inner->compare(lhs, lhs_size, rhs, rhs_size);
    }
    StringRef convert(const char * s, size_t size, String & buffer, std::vector<size_t> * lens) const override
    {
        return inner->convert(s, size, buffer, lens);
    }
    StringRef sortKeyNoTrim(const char * s, size_t size, String & buffer) const override
    {
        return inner->sortKeyNoTrim(s, size, buffer);
    }
    StringRef sortKey(const char * s, size_t size, String & buffer) const override
    {
        return inner->sortKey(s, size, buffer);
    }
    std::unique_ptr<IPattern> pattern() const override { return inner->pattern(); }

    mutable size_t comparisons = 0;

private:
    TiDB::TiDBCollatorPtr inner;
};

String serializeLocalMatchAgainstBooleanQuery(const tipb::LocalMatchAgainstBooleanQuery & query)
{
    auto encoded_query = query;
    if (encoded_query.version() == 0)
        encoded_query.set_version(2); // The tests exercise the currently supported Local MATCH semantic protocol.
    if (encoded_query.parser() == tipb::LocalMatchAgainstParser::LocalMatchAgainstParserInvalid)
        encoded_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserStandard);
    if (encoded_query.stopword_mode() == tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeInvalid)
        encoded_query.set_stopword_mode(tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin);
    if (encoded_query.stopword_collation().empty())
        encoded_query.set_stopword_collation("utf8mb4_bin");
    return encoded_query.SerializeAsString();
}
} // namespace

TEST_F(TestLocalMatchAgainst, MatchBooleanMultiColumnNullable)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required->set_text("quick");

    const String metadata = serializeLocalMatchAgainstBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1, 1, 0, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(4, "+quick"),
             createColumn<Nullable<String>>({{}, "quick fox", {}, "slow fox"}),
             createColumn<Nullable<String>>({"quick fox", {}, "slow fox", {}}),
             createConstColumn<String>(4, metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, FilteredPhraseVerificationComparisonGrowth)
try
{
    tipb::LocalMatchAgainstBooleanQuery query;
    auto * node = query.add_nodes();
    node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    node->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
    node->set_text("foo a bar");
    size_t small_comparisons = 0;
    for (const size_t repeats : {32, 256})
    {
        String document;
        for (size_t i = 0; i < repeats; ++i)
            document += "foo x bar ";
        CountingMatchCollator collator;
        ASSERT_COLUMN_EQ(
            createColumn<Float64>({0}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(1, "+\"foo a bar\""),
                 createColumn<String>({document}),
                 createConstColumn<String>(1, serializeLocalMatchAgainstBooleanQuery(query))},
                &collator,
                true));
        if (repeats == 32)
            small_comparisons = collator.comparisons;
        else
        {
            EXPECT_GT(small_comparisons, 0);
            // Eight times the input may add proportional token work, not
            // 64 times the work from rechecking every failed anchor.
            EXPECT_LE(collator.comparisons, small_comparisons * 12);
        }
    }
}
CATCH

TEST_F(TestLocalMatchAgainst, FilteredPhraseVerificationCacheScope)
try
{
    String miss;
    for (size_t i = 0; i < 64; ++i)
        miss += "foo x bar ";
    for (const auto occur :
         {tipb::LocalMatchAgainstBooleanOccurMust,
          tipb::LocalMatchAgainstBooleanOccurShould,
          tipb::LocalMatchAgainstBooleanOccurMustNot})
    {
        tipb::LocalMatchAgainstBooleanQuery query;
        if (occur != tipb::LocalMatchAgainstBooleanOccurShould)
        {
            auto * anchor = query.add_nodes();
            anchor->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
            anchor->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
            anchor->set_text("foo");
        }
        auto * phrase = query.add_nodes();
        phrase->set_occur(occur);
        phrase->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
        phrase->set_text("foo a bar");
        const Float64 hit = occur == tipb::LocalMatchAgainstBooleanOccurMustNot ? 0 : 1;
        const Float64 no_phrase = occur == tipb::LocalMatchAgainstBooleanOccurMustNot ? 1 : 0;
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Float64>>({hit, hit, no_phrase, hit, no_phrase}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(5, "foo \"foo a bar\""),
                 createColumn<Nullable<String>>({miss, "foo a bar", miss, {}, miss}),
                 createColumn<Nullable<String>>({"foo a bar", miss, {}, "foo a bar", miss}),
                 createConstColumn<String>(5, serializeLocalMatchAgainstBooleanQuery(query))},
                nullptr,
                true));
    }

    tipb::LocalMatchAgainstBooleanQuery query;
    auto * required = query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
    required->set_text("foo x bar");
    auto * excluded = query.add_nodes();
    excluded->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
    excluded->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
    excluded->set_text("foo a bar");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(3, "+\"foo x bar\" -\"foo a bar\""),
             createColumn<String>({miss, "foo x bar foo a bar", miss}),
             createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(query))},
            nullptr,
            true));

    // NGRAM size 3 removes "the" but retains "foo" and "zoo".
    // STANDARD exercises the same cache with a filtered stopword.
    for (const bool ngram : {false, true})
    {
        tipb::LocalMatchAgainstBooleanQuery filtered;
        filtered.set_parser(ngram ? tipb::LocalMatchAgainstParserNgram : tipb::LocalMatchAgainstParserStandard);
        filtered.set_ngram_token_size(3);
        auto * node = filtered.add_nodes();
        node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        node->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
        node->set_text("foo the zoo");
        String repetitive_miss;
        for (size_t i = 0; i < 64; ++i)
            repetitive_miss += "foo xyz zoo ";
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Float64>>({1, 1, 0, 1}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(4, "+\"foo the zoo\""),
                 createColumn<Nullable<String>>({repetitive_miss, "foo the zoo", repetitive_miss, {}}),
                 createColumn<Nullable<String>>({"foo the zoo", repetitive_miss, {}, "foo the zoo"}),
                 createConstColumn<String>(4, serializeLocalMatchAgainstBooleanQuery(filtered))},
                nullptr,
                true));
    }
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanMySQLCharacterBoundaries)
try
{
    tipb::LocalMatchAgainstBooleanQuery query;
    query.set_parser(tipb::LocalMatchAgainstParserNgram);
    query.set_ngram_token_size(2);
    query.set_stopword_mode(tipb::LocalMatchAgainstStopwordModeDisabled);
    auto * node = query.add_nodes();
    node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    node->set_term_type(tipb::LocalMatchAgainstBooleanTermPrefix);
    node->set_text("a");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1, 1, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(6, "+a*"),
             createColumn<String>({"a,b", "a，b", "a🙃b", "a𞤀b", "a𝟙b", String("ab") + char(0xff) + "cd"}),
             createConstColumn<String>(6, serializeLocalMatchAgainstBooleanQuery(query))},
            nullptr,
            true));

    query.set_parser(tipb::LocalMatchAgainstParserStandard);
    node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    node->set_text("foo");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(4, "+foo"),
             createColumn<String>({"foo𞤀bar", "foo𝟙bar", "foo🙃bar", "foobar"}),
             createConstColumn<String>(4, serializeLocalMatchAgainstBooleanQuery(query))},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanFilteredPhraseVerification)
try
{
    for (const bool ngram : {false, true})
        for (const bool stopwords : {false, true})
        {
            tipb::LocalMatchAgainstBooleanQuery query;
            query.set_parser(ngram ? tipb::LocalMatchAgainstParserNgram : tipb::LocalMatchAgainstParserStandard);
            query.set_ngram_token_size(2);
            query.set_stopword_mode(
                stopwords ? tipb::LocalMatchAgainstStopwordModeBuiltin : tipb::LocalMatchAgainstStopwordModeDisabled);
            auto * node = query.add_nodes();
            node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
            node->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
            node->set_text("quick x fox");
            ASSERT_COLUMN_EQ(
                createColumn<Float64>({ngram ? 0.0 : 1.0, 0, 0}),
                executeFunction(
                    "local_match_against_boolean",
                    {createConstColumn<String>(3, "+\"quick x fox\""),
                     createColumn<String>({"quick x fox", "quick the fox", "quick xx fox"}),
                     createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(query))},
                    nullptr,
                    true));
            node->set_text("foo bar");
            ASSERT_COLUMN_EQ(
                createColumn<Float64>({1, 1, ngram ? 0.0 : 1.0, ngram ? 0.0 : 1.0, 0}),
                executeFunction(
                    "local_match_against_boolean",
                    {createConstColumn<String>(5, "+\"foo bar\""),
                     createColumn<String>({"foo bar", "foo,bar", "foo，bar", "foo🙃bar", "foobar"}),
                     createConstColumn<String>(5, serializeLocalMatchAgainstBooleanQuery(query))},
                    nullptr,
                    true));
        }

    tipb::LocalMatchAgainstBooleanQuery query;
    query.set_parser(tipb::LocalMatchAgainstParserNgram);
    query.set_ngram_token_size(3);
    query.set_stopword_mode(tipb::LocalMatchAgainstStopwordModeBuiltin);
    auto * node = query.add_nodes();
    node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    node->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
    for (const String & text : {String("quick the fox"), String("quick a fox")})
    {
        node->set_text(text);
        ASSERT_COLUMN_EQ(
            createColumn<Float64>({1, 1, 1}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(3, text),
                 createColumn<String>({"quick fox", "quick x fox", "fox quick"}),
                 createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(query))},
                nullptr,
                true));
    }
    for (const String & text : {String("foo a zoo"), String("foo zoo")})
    {
        node->set_text(text);
        const Float64 expected = text == "foo zoo" ? 1.0 : 0.0;
        ASSERT_COLUMN_EQ(
            createColumn<Float64>({expected, expected, expected, 0}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(4, text),
                 createColumn<String>({"foo zoo", "foo a zoo", "foo x zoo", "zoo foo"}),
                 createConstColumn<String>(4, serializeLocalMatchAgainstBooleanQuery(query))},
                nullptr,
                true));
    }
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanFilteredRequiredClause)
try
{
    for (const bool ngram : {false, true})
        for (const String & term : {String("the"), String("x")})
        {
            tipb::LocalMatchAgainstBooleanQuery query;
            query.set_parser(ngram ? tipb::LocalMatchAgainstParserNgram : tipb::LocalMatchAgainstParserStandard);
            query.set_ngram_token_size(3);
            auto * required = query.add_nodes();
            required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
            required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
            required->set_text(term);
            auto * optional = query.add_nodes();
            optional->set_occur(tipb::LocalMatchAgainstBooleanOccurShould);
            optional->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
            optional->set_text("quick");
            const auto columns = ColumnsWithTypeAndName{
                createConstColumn<String>(3, "+" + term + " quick"),
                createColumn<Nullable<String>>({"the x quick", String(4096, 'x'), {}}),
                createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(query))};
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<Float64>>({0, 0, 0}),
                executeFunction("local_match_against_boolean", columns, nullptr, true));

            // NULL AGAINST is an empty search even for an impossible query.
            auto null_search = columns;
            null_search[0] = createConstColumn<Nullable<String>>(3, {});
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<Float64>>({0, 0, 0}),
                executeFunction("local_match_against_boolean", null_search, nullptr, true));

            // Envelope validation must still reject an unknown version.
            query.set_version(999);
            EXPECT_THROW(
                executeFunction(
                    "local_match_against_boolean",
                    {columns[0],
                     columns[1],
                     createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(query))},
                    nullptr,
                    true),
                Exception);
        }
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanProtocolQuery)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required->set_text("quick");
    auto * prohibited = boolean_query.add_nodes();
    prohibited->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
    prohibited->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    prohibited->set_text("slow");

    const String metadata = serializeLocalMatchAgainstBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(3, "+quick -slow"),
             createColumn<String>({"quick brown", "slow fox", "brown fox"}),
             createConstColumn<String>(3, metadata)},
            nullptr,
            true));

    tipb::LocalMatchAgainstBooleanQuery prohibited_query;
    auto * prohibited_only = prohibited_query.add_nodes();
    prohibited_only->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
    prohibited_only->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    prohibited_only->set_text("slow");
    const String prohibited_metadata = serializeLocalMatchAgainstBooleanQuery(prohibited_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(3, "-slow"),
             createColumn<String>({"quick brown", "slow fox", "brown fox"}),
             createConstColumn<String>(3, prohibited_metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanPhrasePositionsWithStopwordGaps)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
    required->set_text("quick the fox");

    const String metadata = serializeLocalMatchAgainstBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 0, 1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(4, "+\"quick the fox\""),
             createColumn<String>({"quick the fox", "quick fox", "quick slow the fox", "quick the fox quick the fox"}),
             createConstColumn<String>(4, metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanConstantDocumentColumn)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required->set_text("quick");

    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(3, "+quick"),
             createConstColumn<String>(3, "quick fox"),
             createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(boolean_query))},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanMultiColumnStateAndPhraseBoundary)
try
{
    const auto make_query = [](tipb::LocalMatchAgainstBooleanTermType term_type) {
        tipb::LocalMatchAgainstBooleanQuery boolean_query;
        auto * required = boolean_query.add_nodes();
        required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        required->set_term_type(term_type);
        required->set_text("quick fox");
        return serializeLocalMatchAgainstBooleanQuery(boolean_query);
    };

    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+quick +fox"),
             createColumn<String>({"quick", "quick"}),
             createColumn<String>({"fox", "slow"}),
             createConstColumn<String>(2, make_query(tipb::LocalMatchAgainstBooleanTermWord))},
            nullptr,
            true));

    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+\"quick fox\""),
             createColumn<String>({"quick", "quick fox"}),
             createColumn<String>({"fox", "slow"}),
             createConstColumn<String>(2, make_query(tipb::LocalMatchAgainstBooleanTermPhrase))},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanDecidesPositiveMatchEarly)
try
{
    tipb::LocalMatchAgainstBooleanQuery required_query;
    auto * required_quick = required_query.add_nodes();
    required_quick->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required_quick->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required_quick->set_text("quick");
    auto * required_fox = required_query.add_nodes();
    required_fox->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required_fox->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required_fox->set_text("fox");

    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+quick +fox"),
             createColumn<String>({"quick", "quick"}),
             createColumn<String>({"fox", "quiet"}),
             createConstColumn<String>(2, serializeLocalMatchAgainstBooleanQuery(required_query))},
            nullptr,
            true));

    tipb::LocalMatchAgainstBooleanQuery optional_query;
    auto * optional_quick = optional_query.add_nodes();
    optional_quick->set_occur(tipb::LocalMatchAgainstBooleanOccurShould);
    optional_quick->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    optional_quick->set_text("quick");
    auto * optional_fox = optional_query.add_nodes();
    optional_fox->set_occur(tipb::LocalMatchAgainstBooleanOccurShould);
    optional_fox->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    optional_fox->set_text("fox");

    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "quick fox"),
             createColumn<String>({"quick", "quiet"}),
             createConstColumn<String>(2, serializeLocalMatchAgainstBooleanQuery(optional_query))},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanStillChecksProhibitedTermsAfterPositiveMatch)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    auto * required_quick = boolean_query.add_nodes();
    required_quick->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required_quick->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required_quick->set_text("quick");
    auto * required_fox = boolean_query.add_nodes();
    required_fox->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required_fox->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required_fox->set_text("fox");
    auto * prohibited_slow = boolean_query.add_nodes();
    prohibited_slow->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
    prohibited_slow->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    prohibited_slow->set_text("slow");

    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(3, "+quick +fox -slow"),
             createColumn<String>({"quick", "quick", "quick"}),
             createColumn<String>({"fox", "fox", "quiet"}),
             createColumn<String>({"calm", "slow", "slow"}),
             createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(boolean_query))},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanRejectsInvalidProtocol)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    boolean_query.set_version(1); // Version 1 must not be silently reinterpreted as version 2.
    boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserStandard);
    boolean_query.set_stopword_mode(tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin);
    boolean_query.set_stopword_collation("utf8mb4_bin");
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required->set_text("quick");

    ASSERT_THROW(
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+quick"),
             createColumn<String>({"quick brown"}),
             createConstColumn<String>(1, boolean_query.SerializeAsString())},
            nullptr,
            true),
        Exception);

    boolean_query.set_version(2);
    boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserInvalid);
    ASSERT_THROW(
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+quick"),
             createColumn<String>({"quick brown"}),
             createConstColumn<String>(1, boolean_query.SerializeAsString())},
            nullptr,
            true),
        Exception);

    boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserStandard);
    boolean_query.set_stopword_mode(tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeInvalid);
    ASSERT_THROW(
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+quick"),
             createColumn<String>({"quick brown"}),
             createConstColumn<String>(1, boolean_query.SerializeAsString())},
            nullptr,
            true),
        Exception);
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanStandardAnalyzerProtocolSettings)
try
{
    const auto make_metadata = [](const String & term,
                                  UInt32 min_token_size,
                                  UInt32 max_token_size,
                                  bool enable_stopword,
                                  const String & stopword_collation) {
        tipb::LocalMatchAgainstBooleanQuery boolean_query;
        boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserStandard);
        boolean_query.set_stopword_collation(stopword_collation);
        boolean_query.set_innodb_ft_min_token_size(min_token_size);
        boolean_query.set_innodb_ft_max_token_size(max_token_size);
        boolean_query.set_stopword_mode(
            enable_stopword ? tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin
                            : tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeDisabled);
        auto * required = boolean_query.add_nodes();
        required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
        required->set_text(term);
        return serializeLocalMatchAgainstBooleanQuery(boolean_query);
    };

    const auto short_term_metadata = make_metadata("go", 1, 84, true, "utf8mb4_bin");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+go"),
             createColumn<String>({"go", "good"}),
             createConstColumn<String>(2, short_term_metadata)},
            nullptr,
            true));

    const auto stopword_metadata = make_metadata("the", 1, 84, false, "utf8mb4_bin");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+the"),
             createColumn<String>({"the", "there"}),
             createConstColumn<String>(2, stopword_metadata)},
            nullptr,
            true));

    const auto collated_stopword_metadata = make_metadata("thé", 3, 84, true, "utf8mb4_general_ci");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+thé"),
             createColumn<String>({"thé"}),
             createConstColumn<String>(1, collated_stopword_metadata)},
            TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
            true));

    const auto binary_server_metadata = make_metadata("The", 3, 84, true, "utf8mb4_bin");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+The"),
             createColumn<String>({"The"}),
             createConstColumn<String>(1, binary_server_metadata)},
            TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
            true));

    const auto max_size_metadata = make_metadata("extraordinary", 1, 10, false, "utf8mb4_bin");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+extraordinary"),
             createColumn<String>({"extraordinary"}),
             createConstColumn<String>(1, max_size_metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanNgramProtocolQuery)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram);
    boolean_query.set_ngram_token_size(2);
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    required->set_text("数据库");
    auto * prohibited = boolean_query.add_nodes();
    prohibited->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
    prohibited->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    prohibited->set_text("mysql");

    const String metadata = serializeLocalMatchAgainstBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(4, "+数据库 -mysql"),
             createColumn<String>({"数据库系统", "MySQL 数据库", "数据库", "数据科学"}),
             createConstColumn<String>(4, metadata)},
            nullptr,
            true));

    const auto ci_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    tipb::LocalMatchAgainstBooleanQuery case_query;
    case_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram);
    case_query.set_ngram_token_size(2);
    auto * case_term = case_query.add_nodes();
    case_term->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    case_term->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    case_term->set_text("mysql");
    const String case_metadata = serializeLocalMatchAgainstBooleanQuery(case_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+mysql"),
             createColumn<String>({"MySQL", "PostgreSQL"}),
             createConstColumn<String>(2, case_metadata)},
            ci_collator,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanNgramStopwordSettings)
try
{
    const auto make_metadata = [](UInt32 token_size, bool enable_stopword) {
        tipb::LocalMatchAgainstBooleanQuery boolean_query;
        boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram);
        boolean_query.set_ngram_token_size(token_size);
        boolean_query.set_stopword_mode(
            enable_stopword ? tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin
                            : tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeDisabled);
        auto * required = boolean_query.add_nodes();
        required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
        required->set_text("the");
        return serializeLocalMatchAgainstBooleanQuery(boolean_query);
    };

    const auto query = createConstColumn<String>(3, "+the");
    const auto documents = createColumn<String>({"the", "other", "there"});

    // A stopword longer than ngram_token_size is ignored by the NGRAM parser.
    const auto size_two_metadata = createConstColumn<String>(3, make_metadata(2, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, size_two_metadata}, nullptr, true));

    // At size 3, the gram "the" is removed from both query and document.
    const auto enabled_metadata = createConstColumn<String>(3, make_metadata(3, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 0}),
        executeFunction("local_match_against_boolean", {query, documents, enabled_metadata}, nullptr, true));

    tipb::LocalMatchAgainstBooleanQuery filtered_prefix;
    filtered_prefix.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram);
    filtered_prefix.set_ngram_token_size(2);
    filtered_prefix.set_stopword_mode(tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin);
    auto * prefix_node = filtered_prefix.add_nodes();
    prefix_node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    prefix_node->set_term_type(tipb::LocalMatchAgainstBooleanTermPrefix);
    prefix_node->set_text("caf");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+caf*"),
             createColumn<String>({"cafe"}),
             createConstColumn<String>(1, serializeLocalMatchAgainstBooleanQuery(filtered_prefix))},
            nullptr,
            true));

    // Explicit OFF keeps that gram and restores the matches.
    const auto disabled_metadata = createConstColumn<String>(3, make_metadata(3, false));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, disabled_metadata}, nullptr, true));

    // Stopword matching follows collation_server independently from the
    // MATCH column collation.
    tipb::LocalMatchAgainstBooleanQuery collated_query;
    collated_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram);
    collated_query.set_ngram_token_size(2);
    collated_query.set_stopword_mode(tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeBuiltin);
    collated_query.set_stopword_collation("utf8mb4_general_ci");
    auto * collated_required = collated_query.add_nodes();
    collated_required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    collated_required->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    collated_required->set_text("xá");
    const auto collated_metadata = createConstColumn<String>(1, serializeLocalMatchAgainstBooleanQuery(collated_query));
    const auto document = createColumn<String>({"xá"});
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+xá"), document, collated_metadata},
            TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
            true));

    // The MATCH column collation remains accent-insensitive, but the binary
    // server collation does not consider "á" to contain stopword "a".
    collated_query.set_stopword_collation("utf8mb4_bin");
    const auto binary_stopword_metadata
        = createConstColumn<String>(1, serializeLocalMatchAgainstBooleanQuery(collated_query));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(1, "+xá"), document, binary_stopword_metadata},
            TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanNgramPrefix)
try
{
    const auto make_metadata = [](UInt32 token_size, const String & prefix) {
        tipb::LocalMatchAgainstBooleanQuery boolean_query;
        boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserNgram);
        boolean_query.set_ngram_token_size(token_size);
        boolean_query.set_stopword_mode(tipb::LocalMatchAgainstStopwordMode::LocalMatchAgainstStopwordModeDisabled);
        auto * node = boolean_query.add_nodes();
        node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        node->set_term_type(tipb::LocalMatchAgainstBooleanTermPrefix);
        node->set_text(prefix);
        return serializeLocalMatchAgainstBooleanQuery(boolean_query);
    };

    const auto query = createConstColumn<String>(4, "+caf*");
    const auto documents = createColumn<String>({"CAFE", "café", "cafe", "decaf"});
    const auto metadata = createConstColumn<String>(4, make_metadata(2, "caf"));
    const auto utf8mb4_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, metadata}, utf8mb4_bin, true));

    const auto utf8mb4_general_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, metadata}, utf8mb4_general_ci, true));

    const auto short_query = createConstColumn<String>(4, "+c*");
    const auto short_metadata = createConstColumn<String>(4, make_metadata(2, "c"));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {short_query, documents, short_metadata},
            utf8mb4_general_ci,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanCollationMatrix)
try
{
    const auto query = createConstColumn<String>(3, "+cafe");
    const auto documents = createColumn<String>({"CAFE", "café", "cafe"});
    const auto prefix_query = createConstColumn<String>(3, "+caf*");
    const auto make_metadata = [](tipb::LocalMatchAgainstBooleanTermType term_type, const String & term) {
        tipb::LocalMatchAgainstBooleanQuery boolean_query;
        boolean_query.set_parser(tipb::LocalMatchAgainstParser::LocalMatchAgainstParserStandard);
        auto * node = boolean_query.add_nodes();
        node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        node->set_term_type(term_type);
        node->set_text(term);
        return serializeLocalMatchAgainstBooleanQuery(boolean_query);
    };
    const auto word_metadata
        = createConstColumn<String>(3, make_metadata(tipb::LocalMatchAgainstBooleanTermWord, "cafe"));
    const auto prefix_metadata
        = createConstColumn<String>(3, make_metadata(tipb::LocalMatchAgainstBooleanTermPrefix, "caf"));

    const auto utf8mb4_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_bin, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1}),
        executeFunction("local_match_against_boolean", {prefix_query, documents, prefix_metadata}, utf8mb4_bin, true));

    const auto utf8mb4_0900_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_BIN);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_0900_bin, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {prefix_query, documents, prefix_metadata},
            utf8mb4_0900_bin,
            true));

    const auto utf8mb4_general_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_general_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {prefix_query, documents, prefix_metadata},
            utf8mb4_general_ci,
            true));

    const auto utf8mb4_unicode_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_unicode_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {prefix_query, documents, prefix_metadata},
            utf8mb4_unicode_ci,
            true));

    const auto utf8mb4_0900_ai_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_0900_ai_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction(
            "local_match_against_boolean",
            {prefix_query, documents, prefix_metadata},
            utf8mb4_0900_ai_ci,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanSplitWordModifiers)
try
{
    const auto collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    const auto documents = createColumn<String>({"foo only", "bar only", "foo bar", "baz foo", "baz bar", "baz qux"});
    for (const auto occur :
         {tipb::LocalMatchAgainstBooleanOccurShould,
          tipb::LocalMatchAgainstBooleanOccurMust,
          tipb::LocalMatchAgainstBooleanOccurMustNot})
    {
        tipb::LocalMatchAgainstBooleanQuery query;
        auto * node = query.add_nodes();
        node->set_occur(occur);
        node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
        node->set_text("foo.bar");
        if (occur == tipb::LocalMatchAgainstBooleanOccurMustNot)
        {
            auto * positive = query.add_nodes();
            positive->set_occur(tipb::LocalMatchAgainstBooleanOccurShould);
            positive->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
            positive->set_text("baz");
        }
        const auto expected = occur == tipb::LocalMatchAgainstBooleanOccurShould
            ? createColumn<Float64>({1, 1, 1, 1, 1, 0})
            : occur == tipb::LocalMatchAgainstBooleanOccurMust ? createColumn<Float64>({0, 0, 1, 0, 0, 0})
                                                               : createColumn<Float64>({0, 0, 0, 0, 0, 1});
        const auto metadata = createConstColumn<String>(6, serializeLocalMatchAgainstBooleanQuery(query));
        ASSERT_COLUMN_EQ(
            expected,
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(6, "foo.bar"), documents, metadata},
                collator,
                true));
    }
    // The intersection for a required split word may span MATCH columns.
    tipb::LocalMatchAgainstBooleanQuery query;
    auto * node = query.add_nodes();
    node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
    node->set_text("foo.bar");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "local_match_against_boolean",
            {createConstColumn<String>(2, "+foo.bar"),
             createColumn<String>({"foo", "foo"}),
             createColumn<String>({"bar", "qux"}),
             createConstColumn<String>(2, serializeLocalMatchAgainstBooleanQuery(query))},
            collator,
            true));
}
CATCH

TEST_F(TestLocalMatchAgainst, BuiltinStopwordLookupMatchesComparisons)
try
{
    // Independent reference: retain the old compare-based stopword rule,
    // including NGRAM's substring and source-code-point-length restriction.
    constexpr std::array<std::string_view, 35> stopwords{
        "a",    "about", "an",  "are",  "as",   "at",    "be",  "by",   "com",  "de",  "en",   "for",
        "from", "how",   "i",   "in",   "is",   "it",    "la",  "of",   "on",   "or",  "that", "the",
        "this", "to",    "was", "what", "when", "where", "who", "will", "with", "und", "www"};
    std::vector<String> candidates;
    for (const auto word : stopwords)
        candidates.emplace_back(word);
    for (const String & word :
         {"ABOUT", "The", "THE", "WWW", "WITH", "ábout", "thé", "tĥe", "ｗｉｔｈ", "ｗｗｗ", "aß",    "ß",
          "xá",    "áx",  "xＡ", "Ａx", "xáy",  "aaa",   "AAA", "xy",  "xyz",      "foo",    "数据库"})
        candidates.push_back(word);
    for (const auto collation :
         {"binary",
          "ascii_bin",
          "latin1_bin",
          "utf8_bin",
          "utf8mb4_bin",
          "utf8mb4_0900_bin",
          "utf8_general_ci",
          "utf8mb4_general_ci",
          "utf8_unicode_ci",
          "utf8mb4_unicode_ci",
          "utf8mb4_0900_ai_ci"})
    {
        const auto stopword_collator = TiDB::ITiDBCollator::getCollator(collation);
        ASSERT_NE(stopword_collator, nullptr);
        // Check the key-equality premise directly, including padding and
        // combining marks which the STANDARD tokenizer may split away.
        String key_buffer;
        auto key_copy = [&](const String & text) {
            const auto key = stopword_collator->sortKeyFastPath(text.data(), text.size(), key_buffer);
            return String(key.data, key.size);
        };
        for (const auto word : stopwords)
        {
            const String source(word);
            const auto source_key = key_copy(source);
            for (const auto & candidate : candidates)
                for (const String & suffix : {"", " ", "  ", "\u0301"})
                {
                    const String text = candidate + suffix;
                    EXPECT_EQ(
                        source_key == key_copy(text),
                        stopword_collator->compare(source.data(), source.size(), text.data(), text.size()) == 0)
                        << collation << " source=" << source << " candidate=" << text;
                }
        }
        auto equals_stopword = [&](std::string_view token, size_t length, bool restrict_length) {
            return std::any_of(stopwords.begin(), stopwords.end(), [&](const auto word) {
                return (!restrict_length || length == word.size())
                    && stopword_collator->compare(token.data(), token.size(), word.data(), word.size()) == 0;
            });
        };
        for (const auto size : {0U, 1U, 2U, 3U})
            for (const auto & candidate : candidates)
            {
                SCOPED_TRACE(fmt::format("stopword_collation={} size={} candidate={}", collation, size, candidate));
                std::vector<size_t> boundaries{0};
                for (size_t offset = 0; offset < candidate.size();)
                {
                    offset += UTF8::utf8Decode(candidate.data() + offset, candidate.size() - offset).second;
                    boundaries.push_back(offset);
                }
                bool retained = size == 0 && !equals_stopword(candidate, boundaries.size() - 1, false);
                if (size != 0)
                    for (size_t start = 0; start + size < boundaries.size(); ++start)
                    {
                        bool filtered = false;
                        for (size_t begin = start; begin < start + size; ++begin)
                            for (size_t end = begin + 1; end <= start + size; ++end)
                                filtered = filtered
                                    || equals_stopword(
                                               std::string_view(candidate).substr(
                                                   boundaries[begin],
                                                   boundaries[end] - boundaries[begin]),
                                               end - begin,
                                               true);
                        retained = retained || !filtered;
                    }
                tipb::LocalMatchAgainstBooleanQuery query;
                query.set_parser(
                    size == 0 ? tipb::LocalMatchAgainstParserStandard : tipb::LocalMatchAgainstParserNgram);
                query.set_ngram_token_size(size);
                query.set_innodb_ft_min_token_size(1);
                query.set_innodb_ft_max_token_size(84);
                query.set_stopword_collation(collation);
                auto * node = query.add_nodes();
                node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
                node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
                node->set_text(candidate);
                // Repeated rows exercise key-buffer reuse. Column and server
                // collations are deliberately independent.
                for (const auto match_collation : {"utf8mb4_bin", "utf8mb4_general_ci"})
                    ASSERT_COLUMN_EQ(
                        createColumn<Float64>({retained ? 1.0 : 0.0, retained ? 1.0 : 0.0}),
                        executeFunction(
                            "local_match_against_boolean",
                            {createConstColumn<String>(2, candidate),
                             createColumn<String>({candidate, candidate}),
                             createConstColumn<String>(2, serializeLocalMatchAgainstBooleanQuery(query))},
                            TiDB::ITiDBCollator::getCollator(match_collation),
                            true));
            }
    }
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanASCIIAndMalformedUTF8)
try
{
    const auto collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    const auto documents = createColumn<String>(
        {"bar",
         "foo bar",
         "foobar",
         "bar!",
         String("bar\0foo", 7),
         "bar\xff"
         "foo",
         "foo\xff"
         "bar",
         "foo中bar",
         ""});
    for (const auto size : {0U, 2U, 3U})
    {
        tipb::LocalMatchAgainstBooleanQuery query;
        query.set_parser(size == 0 ? tipb::LocalMatchAgainstParserStandard : tipb::LocalMatchAgainstParserNgram);
        query.set_ngram_token_size(size);
        query.set_stopword_mode(tipb::LocalMatchAgainstStopwordModeDisabled);
        auto * node = query.add_nodes();
        node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
        node->set_text("bar");
        ASSERT_COLUMN_EQ(
            size == 0 ? createColumn<Float64>({1, 1, 0, 1, 1, 1, 1, 0, 0})
                      : createColumn<Float64>({1, 1, 1, 1, 1, 1, 0, 1, 0}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(9, "+bar"),
                 documents,
                 createConstColumn<String>(9, serializeLocalMatchAgainstBooleanQuery(query))},
                collator,
                true));
    }
}
CATCH

TEST_F(TestLocalMatchAgainst, DocumentViewsPreserveFilteredPhrase)
try
{
    String miss;
    for (size_t i = 0; i < 4096; ++i)
        miss += "foo xyz zoo ";
    for (const bool ngram : {false, true})
        for (const bool stopwords : {false, true})
            for (const auto collation : {"utf8mb4_bin", "utf8mb4_general_ci"})
            {
                SCOPED_TRACE(fmt::format("ngram={} stopwords={} collation={}", ngram, stopwords, collation));
                tipb::LocalMatchAgainstBooleanQuery query;
                query.set_parser(ngram ? tipb::LocalMatchAgainstParserNgram : tipb::LocalMatchAgainstParserStandard);
                query.set_ngram_token_size(3);
                query.set_stopword_mode(
                    stopwords ? tipb::LocalMatchAgainstStopwordModeBuiltin
                              : tipb::LocalMatchAgainstStopwordModeDisabled);
                auto * phrase = query.add_nodes();
                phrase->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
                phrase->set_term_type(tipb::LocalMatchAgainstBooleanTermPhrase);
                phrase->set_text("foo the zoo");
                auto * excluded = query.add_nodes();
                excluded->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
                excluded->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
                excluded->set_text("blocked");
                ASSERT_COLUMN_EQ(
                    createColumn<Nullable<Float64>>({1, 1, 1, 0, 0}),
                    executeFunction(
                        "local_match_against_boolean",
                        {createConstColumn<String>(5, "+\"foo the zoo\" -blocked"),
                         createColumn<Nullable<String>>({miss, "foo the zoo", {}, "", miss}),
                         createColumn<Nullable<String>>(
                             {"foo the zoo", miss, "foo the zoo", "foo the zoo blocked", miss}),
                         createConstColumn<String>(5, serializeLocalMatchAgainstBooleanQuery(query))},
                        TiDB::ITiDBCollator::getCollator(collation),
                        true));
            }
}
CATCH

TEST_F(TestLocalMatchAgainst, NgramDocumentViewsAndPositions)
try
{
    // Overlapping grams in separate runs still have consecutive positions;
    // short runs contribute none. Exercise vector growth, scratch reuse and
    // successive large/small/NULL documents without borrowing scratch memory.
    for (const auto size : {1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U, 10U})
        for (const bool chinese : {false, true})
            for (const auto collation : {"", "utf8mb4_bin", "utf8mb4_general_ci"})
            {
                SCOPED_TRACE(fmt::format("size={} chinese={} collation={}", size, chinese, collation));
                const String alphabet = chinese ? "甲乙丙丁戊己庚辛壬癸子丑" : "ABCDEFGHIJKL";
                const size_t width = chinese ? 3 : 1;
                const String query_text = alphabet.substr(0, (size + 1) * width);
                const String first = query_text.substr(0, size * width);
                const String last = query_text.substr(width);
                const String short_run(size - 1, 'z');
                const String padding(4096, 'z');
                const auto documents = createColumn<Nullable<String>>(
                    {padding + " " + query_text,
                     query_text,
                     first + "!" + last,
                     first + "!" + String(size, 'z') + "!" + last,
                     first + "!" + short_run + "!" + last,
                     query_text + "\xff" + padding,
                     first + "\xff" + last,
                     {},
                     "",
                     query_text + "!" + padding});
                tipb::LocalMatchAgainstBooleanQuery query;
                query.set_parser(tipb::LocalMatchAgainstParserNgram);
                query.set_ngram_token_size(size);
                query.set_stopword_mode(tipb::LocalMatchAgainstStopwordModeDisabled);
                auto * node = query.add_nodes();
                node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
                node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
                node->set_text(query_text);
                ASSERT_COLUMN_EQ(
                    createColumn<Nullable<Float64>>({1, 1, 1, 0, 1, 1, 0, 0, 0, 1}),
                    executeFunction(
                        "local_match_against_boolean",
                        {createConstColumn<String>(10, query_text),
                         documents,
                         createConstColumn<String>(10, serializeLocalMatchAgainstBooleanQuery(query))},
                        String(collation).empty() ? nullptr : TiDB::ITiDBCollator::getCollator(collation),
                        true));
            }
}
CATCH

TEST_F(TestLocalMatchAgainst, MatchBooleanUnicodeTokenBoundaries)
try
{
    const auto collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    const auto documents = createColumn<String>(
        {"foo🙃bar",
         "foo👁bar",
         "foo bar",
         "foobar",
         "foo𞤀bar",
         "foo𝟙bar",
         "foo\U0002EBF0bar",
         "foo\xff"
         "bar"});
    for (const auto size : {0U, 2U, 3U})
    {
        tipb::LocalMatchAgainstBooleanQuery query;
        query.set_parser(size == 0 ? tipb::LocalMatchAgainstParserStandard : tipb::LocalMatchAgainstParserNgram);
        query.set_ngram_token_size(size);
        query.set_stopword_mode(tipb::LocalMatchAgainstStopwordModeDisabled);
        auto * node = query.add_nodes();
        node->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
        node->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
        node->set_text(size == 0 ? "foo" : "foob");
        ASSERT_COLUMN_EQ(
            size == 0 ? createColumn<Float64>({1, 1, 1, 0, 1, 1, 1, 1})
                      : createColumn<Float64>({0, 0, 0, 1, 0, 0, 0, 0}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(8, node->text()),
                 documents,
                 createConstColumn<String>(8, serializeLocalMatchAgainstBooleanQuery(query))},
                collator,
                true));
        // TiDB's version-2 NGRAM Boolean lexer emits "bar" for this search.
        node->set_text(size == 0 ? "𞤀bar" : "bar");
        ASSERT_COLUMN_EQ(
            createColumn<Float64>({1, 1, 1}),
            executeFunction(
                "local_match_against_boolean",
                {createConstColumn<String>(3, "+𞤀bar"),
                 createColumn<String>({"foo𞤀bar", "𞤀bar", "foo bar"}),
                 createConstColumn<String>(3, serializeLocalMatchAgainstBooleanQuery(query))},
                collator,
                true));
    }
}
CATCH

TEST(LocalMatchAgainstTokenChars, ProtocolV1Classification)
{
    for (const UInt32 code_point : {0x1F643U, 0x1F441U, 0x301U, 0xFFFDU, 0x2EBF0U, 0x110000U})
        EXPECT_FALSE(LocalMatchAgainst::isTokenChar(code_point)) << code_point;
    for (const UInt32 code_point : {0x5FU, 0x4E2DU, 0xE9U, 0x1D7D9U, 0x1E900U})
        EXPECT_TRUE(LocalMatchAgainst::isTokenChar(code_point)) << code_point;
    UInt32 previous = 127;
    for (const auto & span : LocalMatchAgainst::token_char_ranges)
    {
        ASSERT_GT(span.first, previous);
        ASSERT_LE(span.first, span.last);
        for (UInt32 code_point = previous + 1; code_point < span.first; ++code_point)
            ASSERT_FALSE(LocalMatchAgainst::isTokenChar(code_point)) << code_point;
        for (UInt32 code_point = span.first; code_point <= span.last; ++code_point)
            ASSERT_TRUE(LocalMatchAgainst::isTokenChar(code_point)) << code_point;
        previous = span.last;
    }
    for (UInt32 code_point = previous + 1; code_point <= 0x10FFFFU; ++code_point)
        ASSERT_FALSE(LocalMatchAgainst::isTokenChar(code_point)) << code_point;

    // FNV-1a over one 0/1 byte per code point, U+0000..U+10FFFF. The paired
    // TiDB tests hash Go's Unicode 15.0.0 IsLetter/IsNumber/'_' classification
    // identically. This fixed regression checksum must not change in v1,
    // even when the header is regenerated or either toolchain is upgraded.
    std::uint64_t fingerprint = 14695981039346656037ULL;
    for (UInt32 code_point = 0; code_point <= 0x10FFFFU; ++code_point)
    {
        fingerprint ^= static_cast<std::uint64_t>(LocalMatchAgainst::isTokenChar(code_point));
        fingerprint *= 1099511628211ULL;
    }
    EXPECT_EQ(0x71f51f3810b3b529ULL, fingerprint);
}

} // namespace DB::tests

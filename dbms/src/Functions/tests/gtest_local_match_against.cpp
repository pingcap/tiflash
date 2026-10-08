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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Collation/Collator.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>

namespace DB::tests
{
class TestLocalMatchAgainst : public DB::tests::FunctionTest
{
};

namespace
{
String serializeLocalMatchAgainstBooleanQuery(const tipb::LocalMatchAgainstBooleanQuery & query)
{
    auto encoded_query = query;
    if (encoded_query.version() == 0)
        encoded_query.set_version(1); // The tests exercise the currently supported Local MATCH semantic protocol.
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
             createColumn<String>(
                 {"quick the fox", "quick fox", "quick slow the fox", "quick the fox quick the fox"}),
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

TEST_F(TestLocalMatchAgainst, MatchBooleanRejectsInvalidProtocol)
try
{
    tipb::LocalMatchAgainstBooleanQuery boolean_query;
    boolean_query.set_version(2);
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

    boolean_query.set_version(1);
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
        executeFunction("local_match_against_boolean", {short_query, documents, short_metadata}, utf8mb4_general_ci, true));
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
    const auto word_metadata = createConstColumn<String>(
        3, make_metadata(tipb::LocalMatchAgainstBooleanTermWord, "cafe"));
    const auto prefix_metadata = createConstColumn<String>(
        3, make_metadata(tipb::LocalMatchAgainstBooleanTermPrefix, "caf"));

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
        executeFunction("local_match_against_boolean", {prefix_query, documents, prefix_metadata}, utf8mb4_0900_bin, true));

    const auto utf8mb4_general_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_general_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {prefix_query, documents, prefix_metadata}, utf8mb4_general_ci, true));

    const auto utf8mb4_unicode_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_unicode_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {prefix_query, documents, prefix_metadata}, utf8mb4_unicode_ci, true));

    const auto utf8mb4_0900_ai_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {query, documents, word_metadata}, utf8mb4_0900_ai_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("local_match_against_boolean", {prefix_query, documents, prefix_metadata}, utf8mb4_0900_ai_ci, true));
}
CATCH

} // namespace DB::tests

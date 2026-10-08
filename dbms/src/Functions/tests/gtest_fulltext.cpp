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
class TestFullText : public DB::tests::FunctionTest
{
};

namespace
{
String serializeFTSBooleanQuery(const tipb::FTSBooleanQuery & query)
{
    tipb::FTSMatchBooleanMetadata metadata;
    metadata.set_version(1);
    *metadata.mutable_boolean_query() = query;
    if (metadata.boolean_query().query_tokenizer().empty())
        metadata.mutable_boolean_query()->set_query_tokenizer("STANDARD_V1");
    return metadata.SerializeAsString();
}
} // namespace

TEST_F(TestFullText, MatchBooleanMultiColumnNullable)
try
{
    tipb::FTSBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::FTSBooleanOccurMust);
    required->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    required->mutable_term()->set_text("quick");

    const String metadata = serializeFTSBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1, 1, 0, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(4, "+quick"),
             createColumn<Nullable<String>>({{}, "quick fox", {}, "slow fox"}),
             createColumn<Nullable<String>>({"quick fox", {}, "slow fox", {}}),
             createConstColumn<String>(4, metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestFullText, MatchBooleanProtocolQuery)
try
{
    tipb::FTSBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::FTSBooleanOccurMust);
    required->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    required->mutable_term()->set_text("quick");
    auto * prohibited = boolean_query.add_nodes();
    prohibited->set_occur(tipb::FTSBooleanOccurMustNot);
    prohibited->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    prohibited->mutable_term()->set_text("slow");

    const String metadata = serializeFTSBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(3, "+quick -slow"),
             createColumn<String>({"quick brown", "slow fox", "brown fox"}),
             createConstColumn<String>(3, metadata)},
            nullptr,
            true));

    tipb::FTSBooleanQuery prohibited_query;
    auto * prohibited_only = prohibited_query.add_nodes();
    prohibited_only->set_occur(tipb::FTSBooleanOccurMustNot);
    prohibited_only->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    prohibited_only->mutable_term()->set_text("slow");
    const String prohibited_metadata = serializeFTSBooleanQuery(prohibited_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(3, "-slow"),
             createColumn<String>({"quick brown", "slow fox", "brown fox"}),
             createConstColumn<String>(3, prohibited_metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestFullText, MatchBooleanRejectsInvalidMetadata)
try
{
    tipb::FTSBooleanQuery boolean_query;
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::FTSBooleanOccurMust);
    required->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    required->mutable_term()->set_text("quick");

    tipb::FTSMatchBooleanMetadata metadata;
    metadata.set_version(2);
    *metadata.mutable_boolean_query() = boolean_query;
    ASSERT_THROW(
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(1, "+quick"),
             createColumn<String>({"quick brown"}),
             createConstColumn<String>(1, metadata.SerializeAsString())},
            nullptr,
            true),
        Exception);

    metadata.set_version(1);
    metadata.clear_boolean_query();
    ASSERT_THROW(
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(1, "+quick"),
             createColumn<String>({"quick brown"}),
             createConstColumn<String>(1, metadata.SerializeAsString())},
            nullptr,
            true),
        Exception);
}
CATCH

TEST_F(TestFullText, MatchBooleanStandardAnalyzerProtocolSettings)
try
{
    const auto make_metadata = [](const String & term, UInt32 min_token_size, UInt32 max_token_size, bool enable_stopword) {
        tipb::FTSBooleanQuery boolean_query;
        boolean_query.set_query_tokenizer("STANDARD_V1");
        boolean_query.set_innodb_ft_min_token_size(min_token_size);
        boolean_query.set_innodb_ft_max_token_size(max_token_size);
        boolean_query.set_innodb_ft_enable_stopword(enable_stopword);
        auto * required = boolean_query.add_nodes();
        required->set_occur(tipb::FTSBooleanOccurMust);
        required->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
        required->mutable_term()->set_text(term);
        return serializeFTSBooleanQuery(boolean_query);
    };

    const auto short_term_metadata = make_metadata("go", 1, 84, true);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(2, "+go"),
             createColumn<String>({"go", "good"}),
             createConstColumn<String>(2, short_term_metadata)},
            nullptr,
            true));

    const auto stopword_metadata = make_metadata("the", 1, 84, false);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(2, "+the"),
             createColumn<String>({"the", "there"}),
             createConstColumn<String>(2, stopword_metadata)},
            nullptr,
            true));

    const auto collated_stopword_metadata = make_metadata("thé", 3, 84, true);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(1, "+thé"),
             createColumn<String>({"thé"}),
             createConstColumn<String>(1, collated_stopword_metadata)},
            TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI),
            true));

    const auto max_size_metadata = make_metadata("extraordinary", 1, 10, false);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(1, "+extraordinary"),
             createColumn<String>({"extraordinary"}),
             createConstColumn<String>(1, max_size_metadata)},
            nullptr,
            true));
}
CATCH

TEST_F(TestFullText, MatchBooleanNgramProtocolQuery)
try
{
    tipb::FTSBooleanQuery boolean_query;
    boolean_query.set_query_tokenizer("NGRAM_V1");
    boolean_query.set_ngram_token_size(2);
    auto * required = boolean_query.add_nodes();
    required->set_occur(tipb::FTSBooleanOccurMust);
    required->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    required->mutable_term()->set_text("数据库");
    auto * prohibited = boolean_query.add_nodes();
    prohibited->set_occur(tipb::FTSBooleanOccurMustNot);
    prohibited->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    prohibited->mutable_term()->set_text("mysql");

    const String metadata = serializeFTSBooleanQuery(boolean_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0, 1, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(4, "+数据库 -mysql"),
             createColumn<String>({"数据库系统", "MySQL 数据库", "数据库", "数据科学"}),
             createConstColumn<String>(4, metadata)},
            nullptr,
            true));

    const auto ci_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    tipb::FTSBooleanQuery case_query;
    case_query.set_query_tokenizer("NGRAM_V1");
    case_query.set_ngram_token_size(2);
    auto * case_term = case_query.add_nodes();
    case_term->set_occur(tipb::FTSBooleanOccurMust);
    case_term->mutable_term()->set_term_type(tipb::FTSBooleanTermWord);
    case_term->mutable_term()->set_text("mysql");
    const String case_metadata = serializeFTSBooleanQuery(case_query);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 0}),
        executeFunction(
            "fts_match_boolean_expression",
            {createConstColumn<String>(2, "+mysql"), createColumn<String>({"MySQL", "PostgreSQL"}), createConstColumn<String>(2, case_metadata)},
            ci_collator,
            true));
}
CATCH

TEST_F(TestFullText, MatchBooleanNgramPrefix)
try
{
    const auto make_metadata = [](UInt32 token_size, const String & prefix) {
        tipb::FTSBooleanQuery boolean_query;
        boolean_query.set_query_tokenizer("NGRAM_V1");
        boolean_query.set_ngram_token_size(token_size);
        auto * node = boolean_query.add_nodes();
        node->set_occur(tipb::FTSBooleanOccurMust);
        node->mutable_term()->set_term_type(tipb::FTSBooleanTermPrefix);
        node->mutable_term()->set_text(prefix);
        return serializeFTSBooleanQuery(boolean_query);
    };

    const auto query = createConstColumn<String>(4, "+caf*");
    const auto documents = createColumn<String>({"CAFE", "café", "cafe", "decaf"});
    const auto metadata = createConstColumn<String>(4, make_metadata(2, "caf"));
    const auto utf8mb4_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, metadata}, utf8mb4_bin, true));

    const auto utf8mb4_general_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, metadata}, utf8mb4_general_ci, true));

    const auto short_query = createConstColumn<String>(4, "+c*");
    const auto short_metadata = createConstColumn<String>(4, make_metadata(2, "c"));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {short_query, documents, short_metadata}, utf8mb4_general_ci, true));
}
CATCH

TEST_F(TestFullText, MatchBooleanCollationMatrix)
try
{
    const auto query = createConstColumn<String>(3, "+cafe");
    const auto documents = createColumn<String>({"CAFE", "café", "cafe"});
    const auto prefix_query = createConstColumn<String>(3, "+caf*");
    const auto make_metadata = [](tipb::FTSBooleanTermType term_type, const String & term) {
        tipb::FTSBooleanQuery boolean_query;
        boolean_query.set_query_tokenizer("STANDARD_V1");
        auto * node = boolean_query.add_nodes();
        node->set_occur(tipb::FTSBooleanOccurMust);
        node->mutable_term()->set_term_type(term_type);
        node->mutable_term()->set_text(term);
        return serializeFTSBooleanQuery(boolean_query);
    };
    const auto word_metadata = createConstColumn<String>(
        3, make_metadata(tipb::FTSBooleanTermWord, "cafe"));
    const auto prefix_metadata = createConstColumn<String>(
        3, make_metadata(tipb::FTSBooleanTermPrefix, "caf"));

    const auto utf8mb4_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_BIN);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, word_metadata}, utf8mb4_bin, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1}),
        executeFunction("fts_match_boolean_expression", {prefix_query, documents, prefix_metadata}, utf8mb4_bin, true));

    const auto utf8mb4_0900_bin = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_BIN);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 0, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, word_metadata}, utf8mb4_0900_bin, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({0, 1, 1}),
        executeFunction("fts_match_boolean_expression", {prefix_query, documents, prefix_metadata}, utf8mb4_0900_bin, true));

    const auto utf8mb4_general_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, word_metadata}, utf8mb4_general_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {prefix_query, documents, prefix_metadata}, utf8mb4_general_ci, true));

    const auto utf8mb4_unicode_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, word_metadata}, utf8mb4_unicode_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {prefix_query, documents, prefix_metadata}, utf8mb4_unicode_ci, true));

    const auto utf8mb4_0900_ai_ci = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI);
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {query, documents, word_metadata}, utf8mb4_0900_ai_ci, true));
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({1, 1, 1}),
        executeFunction("fts_match_boolean_expression", {prefix_query, documents, prefix_metadata}, utf8mb4_0900_ai_ci, true));
}
CATCH

} // namespace DB::tests

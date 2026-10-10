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

#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Collation/Collator.h>
#include <benchmark/benchmark.h>
#include <tipb/executor.pb.h>

#include <array>
#include <exception>

namespace DB
{
void registerFunctionsLocalMatchAgainst(FunctionFactory & factory);
}

namespace DB::tests
{
namespace
{
String benchmarkDocument(size_t row, size_t size)
{
    // Keep identical to tests/ftse2e/benchdata in the paired TiDB checkout.
    static const std::array<String, 4> units{
        "quick brown fox prefix 数据库 ",
        "quick slow fox prefix 数据库 ",
        "QUICK brown FOX PREFIX 数据库 ",
        "other words unrelated sample 文档 "};
    const auto & unit = units[row % units.size()];
    String result;
    result.reserve(size);
    for (size_t i = 0; i < size / unit.size(); ++i)
        result += unit;
    result.append(size % unit.size(), ' ');
    return result;
}

void BM_LocalMatchAgainst(benchmark::State & state)
try
{
    const auto bytes = static_cast<size_t>(state.range(0));
    const auto parser = state.range(1); // 0=STANDARD, 2/3=NGRAM token size.
    const bool ci = state.range(2);
    const bool stopwords = state.range(3);
    const auto scenario = state.range(4); // Required/excluded, phrase, prefix, filtered MUST.
    constexpr size_t rows = 16;
    tipb::LocalMatchAgainstBooleanQuery query;
    query.set_version(2);
    query.set_parser(parser == 0 ? tipb::LocalMatchAgainstParserStandard : tipb::LocalMatchAgainstParserNgram);
    query.set_ngram_token_size(parser == 0 ? 2 : parser);
    query.set_innodb_ft_min_token_size(3);
    query.set_innodb_ft_max_token_size(84);
    query.set_stopword_mode(
        stopwords ? tipb::LocalMatchAgainstStopwordModeBuiltin : tipb::LocalMatchAgainstStopwordModeDisabled);
    query.set_stopword_collation(ci ? "utf8mb4_general_ci" : "utf8mb4_bin");
    auto * required = query.add_nodes();
    required->set_occur(tipb::LocalMatchAgainstBooleanOccurMust);
    required->set_term_type(
        scenario == 1       ? tipb::LocalMatchAgainstBooleanTermPhrase
            : scenario == 2 ? tipb::LocalMatchAgainstBooleanTermPrefix
                            : tipb::LocalMatchAgainstBooleanTermWord);
    required->set_text(scenario == 1 ? "quick brown fox" : scenario == 2 ? "pre" : scenario == 3 ? "the" : "quick");
    if (scenario == 0)
    {
        auto * prohibited = query.add_nodes();
        prohibited->set_occur(tipb::LocalMatchAgainstBooleanOccurMustNot);
        prohibited->set_term_type(tipb::LocalMatchAgainstBooleanTermWord);
        prohibited->set_text("slow");
    }
    static const std::array<String, 4> searches{"+quick -slow", "+\"quick brown fox\"", "+pre*", "+the"};
    std::vector<String> documents;
    for (size_t row = 0; row < rows; ++row)
        documents.push_back(benchmarkDocument(row, bytes));
    auto context = TiFlashTestEnv::getContext();
    const ColumnsWithTypeAndName arguments{
        createConstColumn<String>(rows, searches[scenario]),
        createColumn<String>(documents),
        createConstColumn<String>(rows, query.SerializeAsString())};
    const auto collator = TiDB::ITiDBCollator::getCollator(
        ci ? TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI : TiDB::ITiDBCollator::UTF8MB4_BIN);
    auto & factory = FunctionFactory::instance();
    if (!factory.tryGet("local_match_against_boolean", *context))
        registerFunctionsLocalMatchAgainst(factory);
    auto function = factory.get("local_match_against_boolean", *context)->build(arguments, collator);
    Block block(arguments);
    block.insert({nullptr, function->getReturnType(), "result"});
    const ColumnNumbers argument_numbers{0, 1, 2};
    // Inputs and function setup are outside timing. Query compilation remains
    // inside execution, exactly as in the production block-level scalar path.
    for (auto _ : state)
    {
        function->execute(block, argument_numbers, 3);
        benchmark::DoNotOptimize(block.getByPosition(3).column);
    }
    state.SetItemsProcessed(state.iterations() * rows);
    state.SetBytesProcessed(state.iterations() * rows * bytes);
}
catch (const Exception & e)
{
    state.SkipWithError(e.displayText().c_str());
}
catch (const std::exception & e)
{
    state.SkipWithError(e.what());
}

void benchmarkArguments(benchmark::internal::Benchmark * benchmark)
{
    for (const int bytes : {128, 4096, 262144})
        for (const int parser : {0, 2, 3})
            for (const int ci : {0, 1})
                for (const int stopwords : {0, 1})
                    for (const int scenario : {0, 1, 2, 3})
                        benchmark->Args({bytes, parser, ci, stopwords, scenario});
}
BENCHMARK(BM_LocalMatchAgainst)->Apply(benchmarkArguments);
} // namespace
} // namespace DB::tests

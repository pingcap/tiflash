// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Index/VectorIndexHNSW/Index.h>
#include <Storages/DeltaMerge/tests/bench_vector_index_utils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

namespace DB::DM::bench
{

static void VectorIndexBuild(::benchmark::State & state)
try
{
    const auto & dataset = DatasetMnist::get();

    auto train_data = dataset.buildDataTrainColumn(/* max_rows= */ 10000);
    auto index_def = dataset.createIndexDef(tipb::VectorIndexKind::HNSW);
    for (auto _ : state)
    {
        auto builder = std::make_unique<VectorIndexHNSWBuilder>(0, index_def);
        builder->addBlock(*train_data, nullptr, []() { return true; });
    }
}
CATCH

static void VectorIndexSearchTop10(::benchmark::State & state)
try
{
    const auto & dataset = DatasetMnist::get();

    auto index_path = DB::tests::TiFlashTestEnv::getTemporaryPath("vector_search_top_10/vector_index.idx");
    VectorIndexBenchUtils::saveVectorIndex<VectorIndexHNSWBuilder>( //
        index_path,
        dataset,
        /* max_rows= */ 10000);

    auto viewer = VectorIndexBenchUtils::viewVectorIndex<VectorIndexHNSWViewer>(index_path, dataset);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, dataset.dataTestSize() - 1);

    for (auto _ : state)
    {
        auto test_index = dist(rng);
        const auto & query_vector = DatasetMnist::get().dataTestAt(test_index);
        auto keys = VectorIndexBenchUtils::queryTopK(viewer, query_vector, 10, state);
        RUNTIME_CHECK(keys.size() == 10);
    }
}
CATCH

static void VectorIndexSearchTop100(::benchmark::State & state)
try
{
    const auto & dataset = DatasetMnist::get();

    auto index_path = DB::tests::TiFlashTestEnv::getTemporaryPath("vector_search_top_10/vector_index.idx");
    VectorIndexBenchUtils::saveVectorIndex<VectorIndexHNSWBuilder>( //
        index_path,
        dataset,
        /* max_rows= */ 10000);

    auto viewer = VectorIndexBenchUtils::viewVectorIndex<VectorIndexHNSWViewer>(index_path, dataset);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, dataset.dataTestSize() - 1);

    for (auto _ : state)
    {
        auto test_index = dist(rng);
        const auto & query_vector = DatasetMnist::get().dataTestAt(test_index);
        auto keys = VectorIndexBenchUtils::queryTopK(viewer, query_vector, 100, state);
        RUNTIME_CHECK(keys.size() == 100);
    }
}
CATCH

BENCHMARK(VectorIndexBuild);

BENCHMARK(VectorIndexSearchTop10);

BENCHMARK(VectorIndexSearchTop100);

} // namespace DB::DM::bench

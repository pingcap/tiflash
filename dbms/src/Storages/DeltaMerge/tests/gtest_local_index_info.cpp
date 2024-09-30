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

#include <Common/FailPoint.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/KVStore/Types.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>

namespace DB::FailPoints
{
extern const char force_not_support_vector_index[];
} // namespace DB::FailPoints
namespace DB::DM::tests
{

TEST(LocalIndexInfoTest, StorageFormatNotSupport)
try
{
    TiDB::TableInfo table_info;
    {
        TiDB::ColumnInfo column_info;
        column_info.name = "vec";
        column_info.id = 100;
        table_info.columns.emplace_back(column_info);
    }

    auto logger = Logger::get();
    LocalIndexInfosPtr index_info = nullptr;
    // check the same
    {
        auto new_index_info = generateLocalIndexInfos(index_info, table_info, logger).new_local_index_infos;
        ASSERT_EQ(new_index_info, nullptr);
        // check again, nothing changed, return nullptr
        ASSERT_EQ(nullptr, generateLocalIndexInfos(new_index_info, table_info, logger).new_local_index_infos);

        // update
        index_info = new_index_info;
    }

    // Add a vector index to the TableInfo.
    TiDB::IndexColumnInfo default_index_col_info;
    default_index_col_info.name = "vec";
    default_index_col_info.length = -1;
    default_index_col_info.offset = 0;
    TiDB::IndexInfo expect_idx;
    {
        expect_idx.id = 1;
        expect_idx.idx_cols.emplace_back(default_index_col_info);
        expect_idx.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });
        table_info.index_infos.emplace_back(expect_idx);
    }

    FailPointHelper::enableFailPoint(FailPoints::force_not_support_vector_index);

    // check the result when storage format not support
    auto new_index_info = generateLocalIndexInfos(index_info, table_info, logger).new_local_index_infos;
    ASSERT_NE(new_index_info, nullptr);
    // always return empty index_info, we need to drop all existing indexes
    ASSERT_TRUE(new_index_info->empty());
}
CATCH

TEST(LocalIndexInfoTest, CheckIndexChanged)
try
{
    TiDB::TableInfo table_info;
    {
        TiDB::ColumnInfo column_info;
        column_info.name = "vec";
        column_info.id = 100;
        table_info.columns.emplace_back(column_info);
    }

    auto logger = Logger::get();
    LocalIndexInfosPtr index_info = nullptr;
    // check the same
    {
        auto new_index_info = generateLocalIndexInfos(index_info, table_info, logger).new_local_index_infos;
        ASSERT_EQ(new_index_info, nullptr);
        // check again, nothing changed, return nullptr
        ASSERT_EQ(nullptr, generateLocalIndexInfos(new_index_info, table_info, logger).new_local_index_infos);

        // update
        index_info = new_index_info;
    }

    // Add a vector index to the TableInfo.
    TiDB::IndexColumnInfo default_index_col_info;
    default_index_col_info.name = "vec";
    default_index_col_info.length = -1;
    default_index_col_info.offset = 0;
    TiDB::IndexInfo expect_idx;
    {
        expect_idx.id = 1;
        expect_idx.idx_cols.emplace_back(default_index_col_info);
        expect_idx.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        });
        table_info.index_infos.emplace_back(expect_idx);
    }

    // check the different
    {
        auto new_index_info = generateLocalIndexInfos(index_info, table_info, logger).new_local_index_infos;
        ASSERT_NE(new_index_info, nullptr);
        ASSERT_EQ(new_index_info->size(), 1);
        const auto & idx = (*new_index_info)[0];
        ASSERT_EQ(IndexType::Vector, idx.type);
        ASSERT_EQ(expect_idx.id, idx.index_id);
        ASSERT_EQ(100, idx.column_id);
        ASSERT_NE(nullptr, idx.index_definition);
        ASSERT_EQ(expect_idx.vector_index->kind, idx.index_definition->kind);
        ASSERT_EQ(expect_idx.vector_index->dimension, idx.index_definition->dimension);
        ASSERT_EQ(expect_idx.vector_index->distance_metric, idx.index_definition->distance_metric);

        // check again, nothing changed, return nullptr
        ASSERT_EQ(nullptr, generateLocalIndexInfos(new_index_info, table_info, logger).new_local_index_infos);

        // update
        index_info = new_index_info;
    }

    // Add another vector index to the TableInfo.
    TiDB::IndexInfo expect_idx2;
    {
        expect_idx2.id = 2; // another index_id
        expect_idx2.idx_cols.emplace_back(default_index_col_info);
        expect_idx2.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 2,
            .distance_metric = tipb::VectorDistanceMetric::COSINE, // another distance
        });
        table_info.index_infos.emplace_back(expect_idx2);
    }
    // check the different
    {
        auto new_index_info = generateLocalIndexInfos(index_info, table_info, logger).new_local_index_infos;
        ASSERT_NE(new_index_info, nullptr);
        ASSERT_EQ(new_index_info->size(), 2);
        const auto & idx0 = (*new_index_info)[0];
        ASSERT_EQ(IndexType::Vector, idx0.type);
        ASSERT_EQ(expect_idx.id, idx0.index_id);
        ASSERT_EQ(100, idx0.column_id);
        ASSERT_NE(nullptr, idx0.index_definition);
        ASSERT_EQ(expect_idx.vector_index->kind, idx0.index_definition->kind);
        ASSERT_EQ(expect_idx.vector_index->dimension, idx0.index_definition->dimension);
        ASSERT_EQ(expect_idx.vector_index->distance_metric, idx0.index_definition->distance_metric);
        const auto & idx1 = (*new_index_info)[1];
        ASSERT_EQ(IndexType::Vector, idx1.type);
        ASSERT_EQ(expect_idx2.id, idx1.index_id);
        ASSERT_EQ(100, idx1.column_id);
        ASSERT_NE(nullptr, idx1.index_definition);
        ASSERT_EQ(expect_idx2.vector_index->kind, idx1.index_definition->kind);
        ASSERT_EQ(expect_idx2.vector_index->dimension, idx1.index_definition->dimension);
        ASSERT_EQ(expect_idx2.vector_index->distance_metric, idx1.index_definition->distance_metric);

        // check again, nothing changed, return nullptr
        ASSERT_EQ(nullptr, generateLocalIndexInfos(new_index_info, table_info, logger).new_local_index_infos);

        // update
        index_info = new_index_info;
    }

    // Remove the second vecotr index and add a new vector index to the TableInfo.
    TiDB::IndexInfo expect_idx3;
    {
        // drop the second index
        table_info.index_infos.pop_back();
        // add a new index
        expect_idx3.id = 3; // another index_id
        expect_idx3.idx_cols.emplace_back(default_index_col_info);
        expect_idx3.vector_index = TiDB::VectorIndexDefinitionPtr(new TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 3,
            .distance_metric = tipb::VectorDistanceMetric::COSINE, // another distance
        });
        table_info.index_infos.emplace_back(expect_idx3);
    }
    // check the different
    {
        auto new_index_info = generateLocalIndexInfos(index_info, table_info, logger).new_local_index_infos;
        ASSERT_NE(new_index_info, nullptr);
        ASSERT_EQ(new_index_info->size(), 2);
        const auto & idx0 = (*new_index_info)[0];
        ASSERT_EQ(IndexType::Vector, idx0.type);
        ASSERT_EQ(expect_idx.id, idx0.index_id);
        ASSERT_EQ(100, idx0.column_id);
        ASSERT_NE(nullptr, idx0.index_definition);
        ASSERT_EQ(expect_idx.vector_index->kind, idx0.index_definition->kind);
        ASSERT_EQ(expect_idx.vector_index->dimension, idx0.index_definition->dimension);
        ASSERT_EQ(expect_idx.vector_index->distance_metric, idx0.index_definition->distance_metric);
        const auto & idx1 = (*new_index_info)[1];
        ASSERT_EQ(IndexType::Vector, idx1.type);
        ASSERT_EQ(expect_idx3.id, idx1.index_id);
        ASSERT_EQ(100, idx1.column_id);
        ASSERT_NE(nullptr, idx1.index_definition);
        ASSERT_EQ(expect_idx3.vector_index->kind, idx1.index_definition->kind);
        ASSERT_EQ(expect_idx3.vector_index->dimension, idx1.index_definition->dimension);
        ASSERT_EQ(expect_idx3.vector_index->distance_metric, idx1.index_definition->distance_metric);

        // check again, nothing changed, return nullptr
        ASSERT_EQ(nullptr, generateLocalIndexInfos(new_index_info, table_info, logger).new_local_index_infos);
    }
}
CATCH

} // namespace DB::DM::tests

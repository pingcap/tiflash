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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <gtest/gtest.h>


namespace DB::DM::tests
{

class ColumnFileCloneTest : public SegmentTestBasic
{
};

TEST_F(ColumnFileCloneTest, CloneColumnFileTinyWithVectorIndex)
{
    WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());
    const PageIdU64 index_page_id = dm_context->storage_pool->newLogPageId();
    const PageIdU64 data_page_id = dm_context->storage_pool->newLogPageId();
    String mock_data_page_content = "mock_data_page_content";
    wbs.log.putPage(data_page_id, 0, std::string_view{mock_data_page_content.data(), mock_data_page_content.size()});
    String mock_index_page_content = "mock_index_page_content";
    wbs.log.putPage(index_page_id, 0, std::string_view{mock_index_page_content.data(), mock_index_page_content.size()});

    dtpb::VectorIndexFileProps index_props;
    index_props.set_index_kind(tipb::VectorIndexKind_Name(tipb::VectorIndexKind::HNSW));
    index_props.set_distance_metric(tipb::VectorDistanceMetric_Name(tipb::VectorDistanceMetric::L2));
    index_props.set_dimensions(1);
    index_props.set_index_id(1);
    index_props.set_index_bytes(10);
    std::vector<ColumnFilePersistedPtr> persisted_files;
    {
        auto index_infos = std::make_shared<ColumnFileTiny::IndexInfos>();
        index_infos->emplace_back(data_page_id, index_props);
        auto file = std::make_shared<ColumnFileTiny>(nullptr, 1, 10, data_page_id, *dm_context, index_infos);
        persisted_files.emplace_back(std::move(file));
    }
    auto range = RowKeyRange::newAll(dm_context->is_common_handle, dm_context->rowkey_column_size);
    auto new_persisted_files
        = CloneColumnFilesHelper<ColumnFilePersistedPtr>::clone(*dm_context, persisted_files, range, wbs);
    wbs.writeAll();
    ASSERT_EQ(new_persisted_files.size(), 1);
    auto new_persisted_file = new_persisted_files[0];
    auto new_tiny_file = std::dynamic_pointer_cast<ColumnFileTiny>(new_persisted_file);
    ASSERT_NE(new_tiny_file, nullptr);
    // Different data page id
    ASSERT_NE(new_tiny_file->getDataPageId(), data_page_id);
    ASSERT_EQ(new_tiny_file->getIndexInfos()->size(), 1);
    auto new_index_info = new_tiny_file->getIndexInfos()->at(0);
    // Different index page id
    ASSERT_NE(new_index_info.index_page_id, index_page_id);
    // Same index properties
    ASSERT_EQ(new_index_info.vector_index->index_bytes(), 10);
    ASSERT_EQ(new_index_info.vector_index->index_id(), 1);
    ASSERT_EQ(new_index_info.vector_index->index_kind(), tipb::VectorIndexKind_Name(tipb::VectorIndexKind::HNSW));
    ASSERT_EQ(
        new_index_info.vector_index->distance_metric(),
        tipb::VectorDistanceMetric_Name(tipb::VectorDistanceMetric::L2));
    ASSERT_EQ(new_index_info.vector_index->dimensions(), 1);

    // Check the data page and index page content
    auto storage_snap = std::make_shared<StorageSnapshot>(
        *dm_context->storage_pool,
        dm_context->getReadLimiter(),
        dm_context->tracing_id,
        /*snapshot_read*/ true);
    auto data_from_storage_snap = ColumnFileDataProviderLocalStoragePool::create(storage_snap);
    {
        Page page = data_from_storage_snap->readTinyData(data_page_id, {});
        ASSERT_EQ(page.data.size(), mock_data_page_content.size());
        ASSERT_EQ(String(page.data.data(), page.data.size()), mock_data_page_content);
    }
    {
        Page page = data_from_storage_snap->readTinyData(index_page_id, {});
        ASSERT_EQ(page.data.size(), mock_index_page_content.size());
        ASSERT_EQ(String(page.data.data(), page.data.size()), mock_index_page_content.data());
    }
}

} // namespace DB::DM::tests

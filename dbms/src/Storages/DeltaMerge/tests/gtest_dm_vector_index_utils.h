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

#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TiDB/Decode/DatumCodec.h>


namespace DB::DM::tests
{

class VectorIndexTestUtils
{
public:
    ColumnID vec_column_id = 100;
    String vec_column_name = "vec";

    /// Create a column with values like [1], [2], [3], ...
    /// Each value is a VectorFloat32 with exactly one dimension.
    static ColumnWithTypeAndName colInt64(std::string_view sequence, const String & name = "", Int64 column_id = 0)
    {
        auto data = genSequence<Int64>(sequence);
        return ::DB::tests::createColumn<Int64>(data, name, column_id);
    }

    static ColumnWithTypeAndName colVecFloat32(std::string_view sequence, const String & name = "", Int64 column_id = 0)
    {
        auto data = genSequence<Int64>(sequence);
        std::vector<Array> data_in_array;
        for (auto & v : data)
        {
            Array vec;
            vec.push_back(static_cast<Float64>(v));
            data_in_array.push_back(vec);
        }
        return ::DB::tests::createVecFloat32Column<Array>(data_in_array, name, column_id);
    }

    static String encodeVectorFloat32(const std::vector<Float32> & vec)
    {
        WriteBufferFromOwnString wb;
        Array arr;
        for (const auto & v : vec)
            arr.push_back(static_cast<Float64>(v));
        EncodeVectorFloat32(arr, wb);
        return wb.str();
    }

    ColumnDefine cdVec() const
    {
        // When used in read, no need to assign vector_index.
        return ColumnDefine(vec_column_id, vec_column_name, ::DB::tests::typeFromString("Array(Float32)"));
    }

    static size_t cleanVectorCacheEntries(const std::shared_ptr<VectorIndexCache> & cache)
    {
        return cache->cleanOutdatedCacheEntries();
    }

    LocalIndexInfosPtr indexInfo(
        TiDB::VectorIndexDefinition definition = TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        })
    {
        const LocalIndexInfos index_infos = LocalIndexInfos{
            LocalIndexInfo{
                .type = IndexType::Vector,
                .index_id = EmptyIndexID,
                .column_id = vec_column_id,
                .index_definition = std::make_shared<TiDB::VectorIndexDefinition>(definition),
            },
        };
        return std::make_shared<LocalIndexInfos>(index_infos);
    }
};

class DeltaMergeStoreVectorBase : public VectorIndexTestUtils
{
public:
    DeltaMergeStorePtr reload()
    {
        auto cols = DMTestEnv::getDefaultColumns();
        cols->push_back(cdVec());
        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = DeltaMergeStore::create(
            *db_context,
            false,
            "test",
            "t_100",
            NullspaceID,
            100,
            /*pk_col_id*/ 0,
            true,
            *cols,
            handle_column_define,
            false,
            1,
            indexInfo(),
            DeltaMergeStore::Settings());
        return s;
    }

    void write(size_t num_rows_write)
    {
        String sequence = fmt::format("[0, {})", num_rows_write);
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of vector for test
            block.insert(colVecFloat32(sequence, vec_column_name, vec_column_id));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    void writeWithVecData(size_t num_rows_write)
    {
        String sequence = fmt::format("[0, {})", num_rows_write);
        Block block;
        {
            block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            // Add a column of vector for test
            block.insert(createVecFloat32Column<Array>(
                {{1.0, 2.0, 3.0}, {0.0, 0.0, 0.0}, {1.0, 2.0, 3.5}},
                vec_column_name,
                vec_column_id));
        }
        store->write(*db_context, db_context->getSettingsRef(), block);
    }

    void read(const RowKeyRange & range, const PushDownExecutorPtr & filter, const ColumnWithTypeAndName & out)
    {
        auto in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            {cdVec()},
            {range},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            filter,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            /*keep_order=*/false)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({vec_column_name}),
            createColumns({
                out,
            }));
    }

    void triggerMergeDelta() const
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        auto dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
        for (const auto & segment : all_segments)
            ASSERT_TRUE(
                store->segmentMergeDelta(*dm_context, segment, DeltaMergeStore::MergeDeltaReason::Manual) != nullptr);
    }

    void waitStableLocalIndexReady() const
    {
        std::vector<SegmentPtr> all_segments;
        {
            std::shared_lock lock(store->read_write_mutex);
            for (const auto & [_, segment] : store->id_to_segment)
                all_segments.push_back(segment);
        }
        for (const auto & segment : all_segments)
            ASSERT_TRUE(store->segmentWaitStableLocalIndexReady(segment));
    }

    ContextPtr db_context;
    DeltaMergeStorePtr store;

protected:
    constexpr static const char * TRACING_NAME = "DeltaMergeStoreVectorTest";
};

} // namespace DB::DM::tests

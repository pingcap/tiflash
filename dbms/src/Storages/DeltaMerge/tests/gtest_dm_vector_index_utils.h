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
#include <Storages/DeltaMerge/Index/IndexInfo.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Decode/DatumCodec.h>


namespace DB::DM::tests
{

class VectorIndexTestUtils
{
public:
    const ColumnID vec_column_id = 100;
    const String vec_column_name = "vec";

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

    ColumnDefine cdVec()
    {
        // When used in read, no need to assign vector_index.
        return ColumnDefine(vec_column_id, vec_column_name, ::DB::tests::typeFromString("Array(Float32)"));
    }

    static size_t cleanVectorCacheEntries(const std::shared_ptr<VectorIndexCache> & cache)
    {
        return cache->cleanOutdatedCacheEntries();
    }

    IndexInfosPtr indexInfo(
        TiDB::VectorIndexDefinition definition = TiDB::VectorIndexDefinition{
            .kind = tipb::VectorIndexKind::HNSW,
            .dimension = 1,
            .distance_metric = tipb::VectorDistanceMetric::L2,
        })
    {
        const IndexInfos index_infos = IndexInfos{
            IndexInfo{
                .type = IndexType::Vector,
                .column_id = vec_column_id,
                .index_definition = std::make_shared<TiDB::VectorIndexDefinition>(definition),
            },
        };
        return std::make_shared<IndexInfos>(index_infos);
    }
};

} // namespace DB::DM::tests

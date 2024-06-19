// Copyright 2023 PingCAP, Inc.
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

#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Common/LRUCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB::DM
{
class MinMaxIndex;
using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;

class MinMaxIndex
{
public:
    MinMaxIndex(
        PaddedPODArray<UInt8> && has_null_marks_,
        PaddedPODArray<UInt8> && has_value_marks_,
        MutableColumnPtr && minmaxes_)
        : has_null_marks(std::move(has_null_marks_))
        , has_value_marks(std::move(has_value_marks_))
        , minmaxes(std::move(minmaxes_))
    {}

    explicit MinMaxIndex(const IDataType & type)
        : minmaxes(type.createColumn())
    {}

    size_t byteSize() const
    {
        // we add 3 * sizeof(PaddedPODArray<UInt8>)
        // because has_null_marks/ has_value_marks / minmaxes are all use PaddedPODArray
        // Thus we need to add the structual memory cost of PaddedPODArray for each of them
        return sizeof(UInt8) * has_null_marks.size() + sizeof(UInt8) * has_value_marks.size() + minmaxes->byteSize()
            + 3 * sizeof(PaddedPODArray<UInt8>);
    }

    void addPack(const IColumn & column, const ColumnVector<UInt8> * del_mark);

    void write(const IDataType & type, WriteBuffer & buf);

    static MinMaxIndexPtr read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit);

    std::pair<Int64, Int64> getIntMinMax(size_t pack_index) const;

    std::pair<std::string, std::string> getIntMinMaxOrNull(size_t pack_index) const;

    std::pair<StringRef, StringRef> getStringMinMax(size_t pack_index) const;

    std::pair<UInt64, UInt64> getUInt64MinMax(size_t pack_index) const;

    template <typename Op>
    RSResults checkCmp(size_t start_pack, size_t pack_count, const Field & value, const DataTypePtr & type) const;

    // In the current implementation, checkIn can return RSResult::All only when the minimum and maximum values are equal.
    // TODO: Dictionary encoding may help.
    // TODO: There are many duplicates in the code of nullable and not nullable.
    // TODO: Avoid copying when checking columns of string type.
    RSResults checkIn(size_t start_pack, size_t pack_count, const std::vector<Field> & values, const DataTypePtr & type)
        const;

    RSResults checkIsNull(size_t start_pack, size_t pack_count) const;

private:
    template <typename Op, typename T>
    RSResults checkCmpImpl(size_t start_pack, size_t pack_count, const Field & value, const DataTypePtr & type) const;
    template <typename Op>
    RSResults checkNullableCmp(size_t start_pack, size_t pack_count, const Field & value, const DataTypePtr & type)
        const;
    template <typename Op, typename T>
    RSResults checkNullableCmpImpl(
        const DB::ColumnNullable & column_nullable,
        const DB::ColumnUInt8 & null_map,
        size_t start_pack,
        size_t pack_count,
        const Field & value,
        const DataTypePtr & type) const;

    template <typename T>
    RSResults checkInImpl(
        size_t start_pack,
        size_t pack_count,
        const std::vector<Field> & values,
        const DataTypePtr & type) const;
    RSResults checkNullableIn(
        size_t start_pack,
        size_t pack_count,
        const std::vector<Field> & values,
        const DataTypePtr & type) const;
    template <typename T>
    RSResults checkNullableInImpl(
        const DB::ColumnNullable & column_nullable,
        const DB::ColumnUInt8 & null_map,
        size_t start_pack,
        size_t pack_count,
        const std::vector<Field> & values,
        const DataTypePtr & type) const;

    PaddedPODArray<UInt8> has_null_marks;
    PaddedPODArray<UInt8> has_value_marks;
    MutableColumnPtr minmaxes;
};

struct MinMaxIndexWeightFunction
{
    size_t operator()(const String & key, const MinMaxIndex & index) const
    {
        auto index_memory_usage = index.byteSize(); // index
        auto cells_memory_usage = 32; // Cells struct memory cost

        // 2. the memory cost of key part
        auto str_len = key.size(); // key_len
        auto key_memory_usage = sizeof(String); // String struct memory cost

        // 3. the memory cost of hash table
        auto unordered_map_memory_usage = 28; // hash table struct approximate memory cost

        // 4. the memory cost of LRUQueue
        auto list_memory_usage = sizeof(std::list<String>); // list struct memory cost

        return index_memory_usage + cells_memory_usage + str_len * 2 + key_memory_usage * 2 + unordered_map_memory_usage
            + list_memory_usage;
    }
};

class MinMaxIndexCache : public LRUCache<String, MinMaxIndex, std::hash<String>, MinMaxIndexWeightFunction>
{
private:
    using Base = LRUCache<String, MinMaxIndex, std::hash<String>, MinMaxIndexWeightFunction>;

public:
    explicit MinMaxIndexCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes)
    {}

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        return result.first;
    }
};

using MinMaxIndexCachePtr = std::shared_ptr<MinMaxIndexCache>;

} // namespace DB::DM

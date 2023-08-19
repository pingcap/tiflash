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
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB
{
namespace DM
{
class MinMaxIndex;
using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;

class MinMaxIndex
{
private:
    using HasValueMarkPtr = std::shared_ptr<PaddedPODArray<UInt8>>;
    using HasNullMarkPtr = std::shared_ptr<PaddedPODArray<UInt8>>;

    HasNullMarkPtr has_null_marks;
    HasValueMarkPtr has_value_marks;
    MutableColumnPtr minmaxes;

private:
    MinMaxIndex(HasNullMarkPtr has_null_marks_, HasValueMarkPtr has_value_marks_, MutableColumnPtr && minmaxes_)
        : has_null_marks(has_null_marks_)
        , has_value_marks(has_value_marks_)
        , minmaxes(std::move(minmaxes_))
    {
    }

public:
    explicit MinMaxIndex(const IDataType & type)
        : has_null_marks(std::make_shared<PaddedPODArray<UInt8>>())
        , has_value_marks(std::make_shared<PaddedPODArray<UInt8>>())
        , minmaxes(type.createColumn())
    {
    }

    size_t byteSize() const
    {
        return sizeof(UInt8) * has_null_marks->size() + sizeof(UInt8) * has_value_marks->size() + minmaxes->byteSize();
    }

    void addPack(const IColumn & column, const ColumnVector<UInt8> * del_mark);

    void write(const IDataType & type, WriteBuffer & buf);

    static MinMaxIndexPtr read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit);

    std::pair<Int64, Int64> getIntMinMax(size_t pack_index);

    std::pair<StringRef, StringRef> getStringMinMax(size_t pack_index);

    std::pair<UInt64, UInt64> getUInt64MinMax(size_t pack_index);

    // TODO: Use has_null and value.isNull to check.

    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type);
    RSResult checkGreater(size_t pack_index, const Field & value, const DataTypePtr & type, int nan_direction);
    RSResult checkGreaterEqual(size_t pack_index, const Field & value, const DataTypePtr & type, int nan_direction);

    static String toString();
};


struct MinMaxIndexWeightFunction
{
    size_t operator()(const MinMaxIndex & index) const { return index.byteSize(); }
};


class MinMaxIndexCache : public LRUCache<String, MinMaxIndex, std::hash<String>, MinMaxIndexWeightFunction>
{
private:
    using Base = LRUCache<String, MinMaxIndex, std::hash<String>, MinMaxIndexWeightFunction>;

public:
    MinMaxIndexCache(size_t max_size_in_bytes, const Delay & expiration_delay)
        : Base(max_size_in_bytes, expiration_delay)
    {}

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        return result.first;
    }
};

using MinMaxIndexCachePtr = std::shared_ptr<MinMaxIndexCache>;

} // namespace DM

} // namespace DB

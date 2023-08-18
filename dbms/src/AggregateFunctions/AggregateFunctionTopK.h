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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Common/FieldVisitors.h>
#include <Common/SpaceSaving.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
// Allow NxK more space before calculating top K to increase accuracy
#define TOP_K_LOAD_FACTOR 3


template <typename T>
struct AggregateFunctionTopKData
{
    using Set = SpaceSaving<T, HashCRC32<T>>;

    Set value;
};


template <typename T>
class AggregateFunctionTopK
    : public IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T>>
{
private:
    using State = AggregateFunctionTopKData<T>;

    UInt64 threshold;
    UInt64 reserved;

public:
    explicit AggregateFunctionTopK(UInt64 threshold)
        : threshold(threshold)
        , reserved(TOP_K_LOAD_FACTOR * threshold)
    {}

    String getName() const override { return "topK"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.insert(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).value.merge(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        auto & set = this->data(place).value;
        set.resize(reserved);
        set.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        auto result_vec = set.topK(threshold);
        size_t size = result_vec.size();

        offsets_to.push_back((offsets_to.empty() ? 0 : offsets_to.back()) + size);

        typename ColumnVector<T>::Container & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = result_vec.begin(); it != result_vec.end(); ++it, ++i)
            data_to[old_size + i] = it->key;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionTopKGenericData
{
    using Set = SpaceSaving<StringRef, StringRefHash>;

    Set value;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns topK() can be implemented more efficently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionTopKGeneric
    : public IAggregateFunctionDataHelper<
          AggregateFunctionTopKGenericData,
          AggregateFunctionTopKGeneric<is_plain_column>>
{
private:
    using State = AggregateFunctionTopKGenericData;

    UInt64 threshold;
    UInt64 reserved;
    DataTypePtr input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionTopKGeneric(UInt64 threshold, const DataTypePtr & input_data_type)
        : threshold(threshold)
        , reserved(TOP_K_LOAD_FACTOR * threshold)
        , input_data_type(input_data_type)
    {}

    String getName() const override { return "topK"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        set.clear();
        set.resize(reserved);

        // Specialized here because there's no deserialiser for StringRef
        size_t count = 0;
        readVarUInt(count, buf);
        for (size_t i = 0; i < count; ++i)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            UInt64 count, error;
            readVarUInt(count, buf);
            readVarUInt(error, buf);
            set.insert(ref, count, error);
            arena->rollback(ref.size);
        }

        set.readAlphaMap(buf);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);

        if constexpr (is_plain_column)
        {
            set.insert(columns[0]->getDataAt(row_num));
        }
        else
        {
            const char * begin = nullptr;
            StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            set.insert(str_serialized);
            arena->rollback(str_serialized.size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).value.merge(this->data(rhs).value);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto result_vec = this->data(place).value.topK(threshold);
        offsets_to.push_back((offsets_to.empty() ? 0 : offsets_to.back()) + result_vec.size());

        for (auto & elem : result_vec)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.key.data, elem.key.size);
            else
                data_to.deserializeAndInsertFromArena(elem.key.data);
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


#undef TOP_K_LOAD_FACTOR

} // namespace DB

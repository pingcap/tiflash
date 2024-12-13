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
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/HashTable/HashSet.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE 0xFFFFFF


namespace DB
{
template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;

    Set value;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T>
class AggregateFunctionGroupUniqArray
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>, AggregateFunctionGroupUniqArray<T>>
{
private:
    using State = AggregateFunctionGroupUniqArrayData<T>;

public:
    String getName() const override { return "groupUniqArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).value.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & key = AggregateFunctionGroupUniqArrayData<T>::Set::Cell::getKey(
            assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
        this->data(place).value.erase(key);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).value.merge(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        auto & set = this->data(place).value;
        size_t size = set.size();
        writeVarUInt(size, buf);
        for (const auto & elem : set)
            writeIntBinary(elem, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).value.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        size_t size = set.size();

        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupUniqArrayGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, INITIAL_SIZE_DEGREE>;

    Set value;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns groupUniqArray() can be implemented more efficently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionGroupUniqArrayGeneric
    : public IAggregateFunctionDataHelper<
          AggregateFunctionGroupUniqArrayGenericData,
          AggregateFunctionGroupUniqArrayGeneric<is_plain_column>>
{
    DataTypePtr input_data_type;

    using State = AggregateFunctionGroupUniqArrayGenericData;

public:
    AggregateFunctionGroupUniqArrayGeneric(const DataTypePtr & input_data_type)
        : input_data_type(input_data_type)
    {}

    String getName() const override { return "groupUniqArray"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        auto & set = this->data(place).value;
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
        {
            writeStringBinary(elem.getValue(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        size_t size;
        readVarUInt(size, buf);
        //TODO: set.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            set.insert(readStringBinaryInto(*arena, buf));
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;

        bool inserted;
        State::Set::LookupResult it;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena)
        const override
    {
        auto & set = this->data(place).value;

        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        auto key = keyHolderGetKey(key_holder);
        set.erase(key);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        bool inserted;
        State::Set::LookupResult it;
        for (auto & rhs_elem : rhs_set)
        {
            // We have to copy the keys to our arena.
            assert(arena != nullptr);
            cur_set.emplace(ArenaKeyHolder{rhs_elem.getValue(), *arena}, it, inserted);
        }
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + set.size());

        for (auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem.getValue(), data_to);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE

} // namespace DB

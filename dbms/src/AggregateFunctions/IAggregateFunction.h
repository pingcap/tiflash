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

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <TiDB/Collation/Collator.h>

#include <cstddef>
#include <memory>
#include <type_traits>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

class Arena;
class Context;
class ReadBuffer;
class WriteBuffer;
class IColumn;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

using AggregateDataPtr = char *;
using ConstAggregateDataPtr = const char *;


/** Aggregate functions interface.
  * Instances of classes with this interface do not contain the data itself for aggregation,
  *  but contain only metadata (description) of the aggregate function,
  *  as well as methods for creating, deleting and working with data.
  * The data resulting from the aggregation (intermediate computing states) is stored in other objects
  *  (which can be created in some memory pool),
  *  and IAggregateFunction is the external interface for manipulating them.
  */
class IAggregateFunction
{
public:
    /// Get main function name.
    virtual String getName() const = 0;

    /// Get the result type.
    virtual DataTypePtr getReturnType() const = 0;

    virtual ~IAggregateFunction() = default;

    /** Data manipulating functions. */

    /** Create empty data for aggregation with `placement new` at the specified location.
      * You will have to destroy them using the `destroy` method.
      */
    virtual void create(AggregateDataPtr __restrict place) const = 0;

    /// Delete data for aggregation.
    virtual void destroy(AggregateDataPtr __restrict place) const noexcept = 0;

    /// It is not necessary to delete data.
    virtual bool hasTrivialDestructor() const = 0;

    /// Get `sizeof` of structure with data.
    virtual size_t sizeOfData() const = 0;

    /// How the data structure should be aligned. NOTE: Currently not used (structures with aggregation state are put without alignment).
    virtual size_t alignOfData() const = 0;

    /** Adds a value into aggregation data on which place points to.
     *  columns points to columns containing arguments of aggregation function.
     *  row_num is number of row which should be added.
     *  Additional parameter arena should be used instead of standard memory allocator if the addition requires memory allocation.
     */
    virtual void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
        = 0;

    /// Merges state (on which place points to) with other state of current aggregation function.
    virtual void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const = 0;

    /// Serializes state (to transmit it over the network, for example).
    virtual void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const = 0;

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const = 0;

    /// Returns true if a function requires Arena to handle own states (see add(), merge(), deserialize()).
    virtual bool allocatesMemoryInArena() const { return false; }

    /// Inserts results into a column.
    virtual void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const = 0;

    /** Returns true for aggregate functions of type -State.
      * They are executed as other aggregate functions, but not finalized (return an aggregation state that can be combined with another).
      */
    virtual bool isState() const { return false; }

    /** The inner loop that uses the function pointer is better than using the virtual function.
      * The reason is that in the case of virtual functions GCC 5.1.2 generates code,
      *  which, at each iteration of the loop, reloads the function address (the offset value in the virtual function table) from memory to the register.
      * This gives a performance drop on simple queries around 12%.
      * After the appearance of better compilers, the code can be removed.
      */
    using AddFunc = void (*)(const IAggregateFunction *, AggregateDataPtr, const IColumn **, size_t, Arena *);
    virtual AddFunc getAddressOfAddFunction() const = 0;

    /** Contains a loop with calls to "add" function. You can collect arguments into array "places"
      *  and do a single call to "addBatch" for devirtualization and inlining.
      */
    virtual void addBatch(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const
        = 0;

    virtual void mergeBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const
        = 0;

    /** The same for single place.
      */
    virtual void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const
        = 0;

    /** The same for single place when need to aggregate only filtered data.
      */
    virtual void addBatchSinglePlaceNotNull(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const
        = 0;

    /** In addition to addBatch, this method collects multiple rows of arguments into array "places"
      *  as long as they are between offsets[i-1] and offsets[i]. This is used for arrayReduce and
      *  -Array combinator. It might also be used generally to break data dependency when array
      *  "places" contains a large number of same values consecutively.
      */
    virtual void addBatchArray(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        const UInt64 * offsets,
        Arena * arena) const
        = 0;

    /** The case when the aggregation key is UInt8
      * and pointers to aggregation states are stored in AggregateDataPtr[256] lookup table.
      */
    virtual void addBatchLookupTable8(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const
        = 0;

    /** This is used for runtime code generation to determine, which header files to include in generated source.
      * Always implement it as
      * const char * getHeaderFilePath() const override { return __FILE__; }
      */
    virtual const char * getHeaderFilePath() const = 0;

    virtual void setCollators(TiDB::TiDBCollators &) {}

    virtual void setContext(const Context &) {}
};

/// Implement method to obtain an address of 'add' function.
template <typename Derived>
class IAggregateFunctionHelper : public IAggregateFunction
{
private:
    static void addFree(
        const IAggregateFunction * that,
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena)
    {
        static_cast<const Derived &>(*that).add(place, columns, row_num, arena);
    }

public:
    AddFunc getAddressOfAddFunction() const override { return &addFree; }

    void addBatch(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        const auto end = start_offset + batch_size;
        static constexpr size_t prefetch_step = 16;
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = start_offset; i < end; ++i)
            {
                const auto place_idx = i - start_offset;
                const auto prefetch_idx = place_idx + prefetch_step;

                if (flags[i] && places[place_idx])
                {
                    if likely (prefetch_idx < end)
                        __builtin_prefetch(places[prefetch_idx] + place_offset);

                    static_cast<const Derived *>(this)->add(places[place_idx] + place_offset, columns, i, arena);
                }
            }
        }
        else
        {
            for (size_t i = start_offset; i < end; ++i)
            {
                const auto place_idx = i - start_offset;
                const auto prefetch_idx = place_idx + prefetch_step;

                if (places[place_idx])
                {
                    if likely (prefetch_idx < end)
                        __builtin_prefetch(places[prefetch_idx] + place_offset);

                    static_cast<const Derived *>(this)->add(places[place_idx] + place_offset, columns, i, arena);
                }
            }
        }
    }

    void mergeBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        for (size_t i = 0; i < batch_size; ++i)
            if (places[i])
                static_cast<const Derived *>(this)->merge(places[i] + place_offset, rhs[i], arena);
    }

    void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
            {
                if (flags[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
                static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
                if (!null_map[i] && flags[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
        else
        {
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
                if (!null_map[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
    }

    void addBatchArray(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        const UInt64 * offsets,
        Arena * arena) const override
    {
        size_t current_offset = 0;
        for (size_t i = 0; i < batch_size; ++i)
        {
            size_t next_offset = offsets[i];
            for (size_t j = current_offset; j < next_offset; ++j)
                if (places[i])
                    static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, j, arena);
            current_offset = next_offset;
        }
    }

    void addBatchLookupTable8(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        static constexpr size_t UNROLL_COUNT = 8;

        size_t i = start_offset;

        size_t batch_size_unrolled = batch_size / UNROLL_COUNT * UNROLL_COUNT;
        for (; i < start_offset + batch_size_unrolled; i += UNROLL_COUNT)
        {
            AggregateDataPtr places[UNROLL_COUNT];
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                AggregateDataPtr & place = map[key[i + j]];
                if (unlikely(!place))
                    init(place);

                places[j] = place;
            }

            for (size_t j = 0; j < UNROLL_COUNT; ++j)
                static_cast<const Derived *>(this)->add(places[j] + place_offset, columns, i + j, arena);
        }

        for (; i < start_offset + batch_size; ++i)
        {
            AggregateDataPtr & place = map[key[i]];
            if (unlikely(!place))
                init(place);
            static_cast<const Derived *>(this)->add(place + place_offset, columns, i, arena);
        }
    }
};

namespace _IAggregateFunctionImpl
{
template <bool with_collator = false>
struct CollatorsHolder
{
    void setCollators(const TiDB::TiDBCollators &) {}

    template <typename T>
    void setDataCollators(T *) const
    {}
};

template <>
struct CollatorsHolder<true>
{
    TiDB::TiDBCollators collators;

    void setCollators(const TiDB::TiDBCollators & collators_) { collators = collators_; }

    template <typename T>
    void setDataCollators(T * data) const
    {
        data->setCollators(collators);
    }
};
} // namespace _IAggregateFunctionImpl

template <bool with_collator = false>
struct AggregationCollatorsWrapper
{
    void setCollators(const TiDB::TiDBCollators &) {}

    StringRef getUpdatedValueForCollator(StringRef & in, size_t) { return in; }

    std::pair<TiDB::TiDBCollatorPtr, std::string *> getCollatorAndSortKeyContainer(size_t)
    {
        return std::make_pair(static_cast<TiDB::TiDBCollatorPtr>(nullptr), &TiDB::dummy_sort_key_contaner);
    }

    void writeCollators(WriteBuffer &) const {}

    void readCollators(ReadBuffer &) {}
};

template <>
struct AggregationCollatorsWrapper<true>
{
    void setCollators(const TiDB::TiDBCollators & collators_)
    {
        collators = collators_;
        sort_key_containers.resize(collators.size());
    }

    StringRef getUpdatedValueForCollator(StringRef & in, size_t column_index)
    {
        if (likely(collators.size() > column_index))
        {
            if (collators[column_index] != nullptr)
                return collators[column_index]->sortKeyFastPath(in.data, in.size, sort_key_containers[column_index]);
            return in;
        }
        else if (collators.empty())
            return in;
        else
            throw Exception("Should not here: collators for aggregation function is not set correctly");
    }

    std::pair<TiDB::TiDBCollatorPtr, std::string *> getCollatorAndSortKeyContainer(size_t index)
    {
        if (likely(index < collators.size()))
            return std::make_pair(collators[index], &sort_key_containers[index]);
        else if (collators.empty())
            return std::make_pair(static_cast<TiDB::TiDBCollatorPtr>(nullptr), &TiDB::dummy_sort_key_contaner);
        else
            throw Exception("Should not here: collators for aggregation function is not set correctly");
    }

    void writeCollators(WriteBuffer & buf) const
    {
        DB::writeBinary(collators.size(), buf);
        for (const auto & collator : collators)
        {
            DB::writeBinary(collator == nullptr ? 0 : collator->getCollatorId(), buf);
        }
    }

    void readCollators(ReadBuffer & buf)
    {
        size_t collator_num;
        DB::readBinary(collator_num, buf);
        sort_key_containers.resize(collator_num);
        for (size_t i = 0; i < collator_num; i++)
        {
            Int32 collator_id;
            DB::readBinary(collator_id, buf);
            if (collator_id != 0)
                collators.push_back(TiDB::ITiDBCollator::getCollator(collator_id));
            else
                collators.push_back(nullptr);
        }
    }

    TiDB::TiDBCollators collators;
    std::vector<std::string> sort_key_containers;
};

/// Implements several methods for manipulation with data. T - type of structure with data for aggregation.
template <typename T, typename Derived, bool with_collator = false>
class IAggregateFunctionDataHelper
    : public IAggregateFunctionHelper<Derived>
    , protected _IAggregateFunctionImpl::CollatorsHolder<with_collator>
{
protected:
    using Data = T;

    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

public:
    void setCollators(TiDB::TiDBCollators & collators_) override
    {
        _IAggregateFunctionImpl::CollatorsHolder<with_collator>::setCollators(collators_);
    }

    void create(AggregateDataPtr __restrict place) const override { this->setDataCollators(new (place) Data); }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { data(place).~Data(); }

    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<Data>; }

    size_t sizeOfData() const override { return sizeof(Data); }

    /// NOTE: Currently not used (structures with aggregation state are put without alignment).
    size_t alignOfData() const override { return alignof(Data); }

    void addBatchLookupTable8(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const Derived & func = *static_cast<const Derived *>(this);

        /// If the function is complex or too large, use more generic algorithm.

        if (func.allocatesMemoryInArena() || sizeof(Data) > 16 || func.sizeOfData() != sizeof(Data))
        {
            IAggregateFunctionHelper<
                Derived>::addBatchLookupTable8(start_offset, batch_size, map, place_offset, init, key, columns, arena);
            return;
        }

        /// Will use UNROLL_COUNT number of lookup tables.

        static constexpr size_t UNROLL_COUNT = 4;

        std::unique_ptr<Data[]> places{new Data[256 * UNROLL_COUNT]};
        bool has_data[256 * UNROLL_COUNT]{}; /// Separate flags array to avoid heavy initialization.

        size_t i = start_offset;

        /// Aggregate data into different lookup tables.

        size_t batch_size_unrolled = batch_size / UNROLL_COUNT * UNROLL_COUNT;
        for (; i < start_offset + batch_size_unrolled; i += UNROLL_COUNT)
        {
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                size_t idx = j * 256 + key[i + j];
                if (unlikely(!has_data[idx]))
                {
                    new (&places[idx]) Data;
                    has_data[idx] = true;
                }
                func.add(reinterpret_cast<char *>(&places[idx]), columns, i + j, nullptr);
            }
        }

        /// Merge data from every lookup table to the final destination.

        for (size_t k = 0; k < 256; ++k)
        {
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                size_t idx = j * 256 + k;
                if (has_data[idx])
                {
                    AggregateDataPtr & place = map[k];
                    if (unlikely(!place))
                        init(place);

                    func.merge(place + place_offset, reinterpret_cast<const char *>(&places[idx]), nullptr);
                }
            }
        }

        /// Process tails and add directly to the final destination.

        for (; i < start_offset + batch_size; ++i)
        {
            size_t k = key[i];
            AggregateDataPtr & place = map[k];
            if (unlikely(!place))
                init(place);

            func.add(place + place_offset, columns, i, nullptr);
        }
    }
};

using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

} // namespace DB

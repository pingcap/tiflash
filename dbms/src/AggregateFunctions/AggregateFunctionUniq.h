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
#include <AggregateFunctions/UniqCombinedBiasData.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <AggregateFunctions/UniquesHashSet.h>
#include <Columns/ColumnString.h>
#include <Common/CombinedCardinalityEstimator.h>
#include <Common/FailPoint.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogWithSmallSetOptimization.h>
#include <Common/MemoryTracker.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <city.h>

#include <type_traits>

namespace DB
{
/// uniq
namespace FailPoints
{
extern const char force_agg_prefetch[];
} // namespace FailPoints

extern const String uniq_raw_res_name;

struct AggregateFunctionUniqUniquesHashSetData : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = UniquesHashSet<DefaultHash<UInt64>>;
    Set set;

    static String getName() { return "uniq"; }
};

/// For a function that takes multiple arguments. Such a function pre-hashes them in advance, so TrivialHash is used here.
struct AggregateFunctionUniqUniquesHashSetDataForVariadic : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = UniquesHashSet<TrivialHash>;
    Set set;

    static String getName() { return "uniq"; }
};

struct AggregateFunctionUniqUniquesHashSetDataForVariadicRawRes : AggregationCollatorsWrapper<true>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = UniquesHashSet<TrivialHash, false>;
    Set set;

    static String getName() { return uniq_raw_res_name; }
};

/// uniqHLL12

template <typename T>
struct AggregateFunctionUniqHLL12Data : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = HyperLogLogWithSmallSetOptimization<T, 16, 12>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<String> : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};

template <>
struct AggregateFunctionUniqHLL12Data<UInt128> : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};

struct AggregateFunctionUniqHLL12DataForVariadic : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Set = HyperLogLogWithSmallSetOptimization<UInt64, 16, 12, TrivialHash>;
    Set set;

    static String getName() { return "uniqHLL12"; }
};


/// uniqExact

template <typename T>
struct AggregateFunctionUniqExactData : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Key = T;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, HashCRC32<Key>>;

    Set set;

    static String getName() { return "uniqExact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <>
struct AggregateFunctionUniqExactData<String> : AggregationCollatorsWrapper<true>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Key = UInt128;

    using Set = HashSet<Key, TrivialHash>;

    Set set;

    static String getName() { return "uniqExact"; }
};

template <typename T>
struct AggregateFunctionUniqCombinedData : AggregationCollatorsWrapper<false>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Key = UInt32;
    using Set = CombinedCardinalityEstimator<
        Key,
        HashSet<Key, TrivialHash, HashTableGrower<>>,
        16,
        14,
        17,
        TrivialHash,
        UInt32,
        HyperLogLogBiasEstimator<UniqCombinedBiasData>,
        HyperLogLogMode::FullFeatured>;

    Set set;

    static String getName() { return "uniqCombined"; }
};

template <>
struct AggregateFunctionUniqCombinedData<String> : AggregationCollatorsWrapper<true>
{
    void write(WriteBuffer & buf) const
    {
        set.write(buf);
        writeCollators(buf);
    }
    void read(ReadBuffer & buf)
    {
        set.read(buf);
        readCollators(buf);
    }
    using Key = UInt64;
    using Set = CombinedCardinalityEstimator<
        Key,
        HashSet<Key, TrivialHash, HashTableGrower<>>,
        16,
        14,
        17,
        TrivialHash,
        UInt64,
        HyperLogLogBiasEstimator<UniqCombinedBiasData>,
        HyperLogLogMode::FullFeatured>;

    Set set;

    static String getName() { return "uniqCombined"; }
};


namespace detail
{
/** Hash function for uniq.
  */
template <typename T>
struct AggregateFunctionUniqTraits
{
    static UInt64 hash(T x) { return x; }
};

template <>
struct AggregateFunctionUniqTraits<UInt128>
{
    static UInt64 hash(UInt128 x) { return sipHash64(x); }
};

template <>
struct AggregateFunctionUniqTraits<Float32>
{
    static UInt64 hash(Float32 x)
    {
        UInt64 res = 0;
        memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
        return res;
    }
};

template <>
struct AggregateFunctionUniqTraits<Float64>
{
    static UInt64 hash(Float64 x)
    {
        UInt64 res = 0;
        memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
        return res;
    }
};

/** Hash function for uniqCombined.
  */
template <typename T>
struct AggregateFunctionUniqCombinedTraits
{
    static UInt32 hash(T x) { return static_cast<UInt32>(intHash64(x)); }
};

template <>
struct AggregateFunctionUniqCombinedTraits<UInt128>
{
    static UInt32 hash(UInt128 x) { return sipHash64(x); }
};

template <>
struct AggregateFunctionUniqCombinedTraits<Float32>
{
    static UInt32 hash(Float32 x)
    {
        UInt64 res = 0;
        memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
        return static_cast<UInt32>(intHash64(res));
    }
};

template <>
struct AggregateFunctionUniqCombinedTraits<Float64>
{
    static UInt32 hash(Float64 x)
    {
        UInt64 res = 0;
        memcpy(reinterpret_cast<char *>(&res), reinterpret_cast<char *>(&x), sizeof(x));
        return static_cast<UInt32>(intHash64(res));
    }
};


/** The structure for the delegation work to add one element to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <typename T, typename Data>
struct OneAdder
{
    static void ALWAYS_INLINE add(Data & data, const IColumn & column, size_t row_num)
    {
        if constexpr (
            std::is_same_v<Data, AggregateFunctionUniqUniquesHashSetData>
            || std::is_same_v<Data, AggregateFunctionUniqHLL12Data<T>>)
        {
            if constexpr (!std::is_same_v<T, String>)
            {
                const auto & value = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
                data.set.insert(AggregateFunctionUniqTraits<T>::hash(value));
            }
            else
            {
                StringRef value = column.getDataAt(row_num);
                value = data.getUpdatedValueForCollator(value, 0);
                data.set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
            }
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqCombinedData<T>>)
        {
            if constexpr (!std::is_same_v<T, String>)
            {
                const auto & value = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
                data.set.insert(AggregateFunctionUniqCombinedTraits<T>::hash(value));
            }
            else
            {
                StringRef value = column.getDataAt(row_num);
                value = data.getUpdatedValueForCollator(value, 0);
                data.set.insert(CityHash_v1_0_2::CityHash64(value.data, value.size));
            }
        }
        else if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>)
        {
            if constexpr (!std::is_same_v<T, String>)
            {
                data.set.insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
            }
            else
            {
                StringRef value = column.getDataAt(row_num);
                value = data.getUpdatedValueForCollator(value, 0);

                UInt128 key;
                SipHash hash;
                hash.update(value.data, value.size);
                hash.get128(key);

                data.set.insert(key);
            }
        }
    }
};

struct BatchAdder
{
public:
    template <typename T, typename Data>
    static ALWAYS_INLINE void addBatchSinglePlace(
        Data & agg_data,
        size_t start_offset,
        size_t batch_size,
        const IColumn ** columns,
        ssize_t if_argument_pos = -1)
    {
        const ColumnUInt8::Container * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = &static_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();

        if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>)
        {
#ifndef NDEBUG
            bool disable_prefetch = (agg_data.set.getBufferSizeInBytes() < agg_prefetch_threshold);
            fiu_do_on(FailPoints::force_agg_prefetch, { disable_prefetch = false; });
#else
            const bool disable_prefetch = (agg_data.set.getBufferSizeInBytes() < agg_prefetch_threshold);
#endif
            LOG_DEBUG(Logger::get(), "gjt debug BatchAdder: disable_prefetch: {}, size: {}",
                    disable_prefetch, agg_data.set.getBufferSizeInBytes());
            if (!disable_prefetch)
            {
                if (flags != nullptr)
                    detail::BatchAdder::addBatchSinglePlaceWithPrefetch</*has_if_argument_pos_data=*/true, T, Data>(
                        start_offset,
                        batch_size,
                        agg_data,
                        columns,
                        flags);
                else
                    detail::BatchAdder::addBatchSinglePlaceWithPrefetch</*has_if_argument_pos_data=*/false, T, Data>(
                        start_offset,
                        batch_size,
                        agg_data,
                        columns,
                        nullptr);
                return;
            }
            // else { fallback to non-prefetch }
        }

        if (flags != nullptr)
            detail::BatchAdder::addBatchSinglePlaceNoPrefetch</*has_if_argument_pos_data=*/true, T, Data>(
                start_offset,
                batch_size,
                agg_data,
                columns,
                flags);
        else
            detail::BatchAdder::addBatchSinglePlaceNoPrefetch</*has_if_argument_pos_data=*/false, T, Data>(
                start_offset,
                batch_size,
                agg_data,
                columns,
                nullptr);
    }

private:
    template <bool has_if_argument_pos_data, typename T, typename Data>
    static void addBatchSinglePlaceWithPrefetch(
        size_t start_offset,
        size_t batch_size,
        Data & agg_data,
        const IColumn ** columns,
        const ColumnUInt8::Container * if_argument_pos_data)
    {
        const size_t mini_batch = 512;
        size_t mini_batch_idx = start_offset;
        const size_t end_row = start_offset + batch_size;
        while (true)
        {
            size_t cur_batch_size = mini_batch;
            if unlikely (mini_batch_idx + cur_batch_size > end_row)
                cur_batch_size = end_row - mini_batch_idx;

            addBatchSinglePlaceWithPrefetchMiniBatch<has_if_argument_pos_data, T, Data>(
                mini_batch_idx,
                cur_batch_size,
                agg_data,
                columns,
                if_argument_pos_data);

            mini_batch_idx += cur_batch_size;
            if unlikely (mini_batch_idx >= end_row)
                break;
        }
    }

    template <bool has_if_argument_pos_data, typename T, typename Data>
    static void addBatchSinglePlaceWithPrefetchMiniBatch(
        size_t start_offset,
        size_t batch_size,
        Data & agg_data,
        const IColumn ** columns,
        const ColumnUInt8::Container * if_argument_pos_data)
    {
        // Other uniq function doesn't support prefetch for now.
        static_assert(std::is_same_v<Data, AggregateFunctionUniqExactData<T>>);

        const auto & arg_column = *columns[0];
        std::vector<size_t> hashvals;
        hashvals.reserve(batch_size);
        std::vector<typename Data::Key> keys;
        keys.reserve(batch_size);

        const size_t end_row = start_offset + batch_size;
        for (size_t row = start_offset; row < end_row; ++row)
        {
            if constexpr (has_if_argument_pos_data)
            {
                if (!(*if_argument_pos_data)[row])
                {
                    hashvals.push_back(0);
                    keys.push_back(typename Data::Key{});
                    continue;
                }
            }

            if constexpr (!std::is_same_v<T, String>)
            {
                const auto & key = static_cast<const ColumnVector<T> &>(arg_column).getData()[row];
                keys.push_back(key);
                hashvals.push_back(agg_data.set.hash(key));
            }
            else
            {
                StringRef value = arg_column.getDataAt(row);
                value = agg_data.getUpdatedValueForCollator(value, 0);

                UInt128 key;
                SipHash hash;
                hash.update(value.data, value.size);
                hash.get128(key);

                keys.push_back(key);
                hashvals.push_back(agg_data.set.hash(key));
            }
        }

        for (size_t i = 0; i < batch_size; ++i)
        {
            if constexpr (has_if_argument_pos_data)
            {
                if (!((*if_argument_pos_data)[start_offset + i]))
                    continue;
            }

            const size_t prefetch_i = i + agg_prefetch_step;
            if likely (prefetch_i < keys.size())
                agg_data.set.prefetch(hashvals[prefetch_i]);

            agg_data.set.insert(keys[i], hashvals[i]);
        }
    }

    template <bool has_if_argument_pos_data, typename T, typename Data>
    static void addBatchSinglePlaceNoPrefetch(
        size_t start_offset,
        size_t batch_size,
        Data & agg_data,
        const IColumn ** columns,
        const ColumnUInt8::Container * if_argument_pos_data)
    {
        for (size_t row = start_offset; row < start_offset + batch_size; ++row)
        {
            if constexpr (has_if_argument_pos_data)
            {
                if ((*if_argument_pos_data)[row])
                    detail::OneAdder<T, Data>::add(agg_data, *columns[0], row);
            }
            else
            {
                detail::OneAdder<T, Data>::add(agg_data, *columns[0], row);
            }
        }
    }
};

struct BatchAdderVariadic
{
public:
    template <bool is_exact, bool argument_is_tuple, typename T, typename Data>
    static ALWAYS_INLINE void addBatchSinglePlace(
        Data & agg_data,
        size_t start_offset,
        size_t batch_size,
        const IColumn ** columns,
        size_t num_args,
        ssize_t if_argument_pos = -1)
    {
        const ColumnUInt8::Container * flags = nullptr;
        if (if_argument_pos >= 0)
            flags = &static_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();

        if constexpr (is_exact && !argument_is_tuple)
        {
#ifndef NDEBUG
            bool disable_prefetch = (agg_data.set.getBufferSizeInBytes() < agg_prefetch_threshold);
            fiu_do_on(FailPoints::force_agg_prefetch, { disable_prefetch = false; });
#else
            const bool disable_prefetch = (agg_data.set.getBufferSizeInBytes() < agg_prefetch_threshold);
#endif
            LOG_DEBUG(Logger::get(), "gjt debug BatchAdder: disable_prefetch: {}, size: {}",
                    disable_prefetch, agg_data.set.getBufferSizeInBytes());
            if (!disable_prefetch)
            {
                if (flags != nullptr)
                    addBatchSinglePlaceWithPrefetch<is_exact, argument_is_tuple, /*has_if_argument_pos_data=*/true>(
                            start_offset,
                            batch_size,
                            agg_data,
                            columns,
                            num_args,
                            flags);
                else
                    addBatchSinglePlaceWithPrefetch<is_exact, argument_is_tuple, /*has_if_argument_pos_data=*/false>(
                            start_offset,
                            batch_size,
                            agg_data,
                            columns,
                            num_args,
                            nullptr);
                return;
            }
            // else { fallback to non-prefetch }
        }

        if (flags != nullptr)
            addBatchSinglePlaceNoPrefetch<is_exact, argument_is_tuple, /*has_if_argument_pos_data=*/true>(
                    start_offset,
                    batch_size,
                    agg_data,
                    columns,
                    num_args,
                    flags);
        else
            addBatchSinglePlaceNoPrefetch<is_exact, argument_is_tuple, /*has_if_argument_pos_data=*/false>(
                    start_offset,
                    batch_size,
                    agg_data,
                    columns,
                    num_args,
                    nullptr);
    }

private:
    template <bool is_exact, bool argument_is_tuple, bool has_if_argument_pos_data, typename Data>
    static void addBatchSinglePlaceWithPrefetch(
        size_t start_offset,
        size_t batch_size,
        Data & agg_data,
        const IColumn ** columns,
        size_t num_args,
        const ColumnUInt8::Container * if_argument_pos_data)
    {
        const size_t mini_batch = 512;
        size_t mini_batch_idx = start_offset;
        const size_t end_row = start_offset + batch_size;

        while (true)
        {
            size_t cur_batch_size = mini_batch;
            if unlikely (mini_batch_idx + cur_batch_size > end_row)
                cur_batch_size = end_row - mini_batch_idx;

            const auto & hash_values = UniqVariadicHash<Data, is_exact, argument_is_tuple>::applyBatch(agg_data, num_args, columns, mini_batch_idx, cur_batch_size);
            for (size_t i = 0; i < cur_batch_size; ++i)
            {
                const size_t row = start_offset + i;
                if constexpr (has_if_argument_pos_data)
                {
                    if (!((*if_argument_pos_data)[row]))
                        continue;
                }

                const size_t prefetch_i = i + agg_prefetch_step;
                // todo trivial hash
                if likely (prefetch_i < hash_values.size())
                    agg_data.set.prefetch(hash_values[prefetch_i].low);

                agg_data.set.insert(hash_values[i], hash_values[i].low);
            }

            mini_batch_idx += cur_batch_size;
            if unlikely (mini_batch_idx >= end_row)
                break;
        }
    }

    template <bool is_exact, bool argument_is_tuple, bool has_if_argument_pos_data, typename Data>
    static void addBatchSinglePlaceNoPrefetch(
        size_t start_offset,
        size_t batch_size,
        Data & agg_data,
        const IColumn ** columns,
        size_t num_args,
        const ColumnUInt8::Container * if_argument_pos_data)
    {
        for (size_t row = start_offset; row < start_offset + batch_size; ++row)
        {
            if constexpr (has_if_argument_pos_data)
            {
                if ((*if_argument_pos_data)[row])
                    agg_data.set.insert(
                            UniqVariadicHash<Data, is_exact, argument_is_tuple>::apply(agg_data, num_args, columns, row));
            }
            else
            {
                agg_data.set.insert(
                        UniqVariadicHash<Data, is_exact, argument_is_tuple>::apply(agg_data, num_args, columns, row));
            }
        }
    }
};
} // namespace detail


/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>, true>
{
public:
    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos = -1) const override
    {
        assert(place);
        auto & agg_data = this->data(place);
        detail::BatchAdder::addBatchSinglePlace<T, Data>(agg_data, start_offset, batch_size, columns, if_argument_pos);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of efficient implementation), you can not pass several arguments, among which there are tuples.
  */
template <typename Data, bool argument_is_tuple, bool raw_result = false>
class AggregateFunctionUniqVariadic final
    : public IAggregateFunctionDataHelper<
          Data,
          AggregateFunctionUniqVariadic<Data, argument_is_tuple, raw_result>,
          true>
{
private:
    static constexpr bool is_exact = std::is_same_v<Data, AggregateFunctionUniqExactData<String>>;

    size_t num_args = 0;

public:
    AggregateFunctionUniqVariadic(const DataTypes & arguments)
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override
    {
        if constexpr (raw_result)
            return std::make_shared<DataTypeString>();
        else
            return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).set.insert(
            UniqVariadicHash<Data, is_exact, argument_is_tuple>::apply(this->data(place), num_args, columns, row_num));
    }

    void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos = -1) const override
    {
        assert(place);
        auto & agg_data = this->data(place);
        detail::BatchAdderVariadic::addBatchSinglePlace<is_exact, argument_is_tuple, Data>(agg_data, start_offset, batch_size, columns, num_args, if_argument_pos);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).set.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if constexpr (raw_result)
        {
            WriteBufferFromOwnString buf;
            serialize(place, buf);
            static_cast<ColumnString &>(to).insertData(buf.str().data(), buf.count());
        }
        else
            static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).set.size());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


} // namespace DB

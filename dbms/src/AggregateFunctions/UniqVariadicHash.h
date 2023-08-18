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

#include <Columns/ColumnTuple.h>
#include <Common/SipHash.h>
#include <Core/Defines.h>
#include <city.h>

namespace DB
{
/** Hashes a set of arguments to the aggregate function
  *  to calculate the number of unique values
  *  and adds them to the set.
  *
  * Four options (2 x 2)
  *
  * - for approximate calculation, uses a non-cryptographic 64-bit hash function;
  * - for an accurate calculation, uses a cryptographic 128-bit hash function;
  *
  * - for several arguments passed in the usual way;
  * - for one argument-tuple.
  */

template <typename Data, bool exact, bool for_tuple>
struct UniqVariadicHash;


template <typename Data>
struct UniqVariadicHash<Data, false, false>
{
    static inline UInt64 apply(Data & data, size_t num_args, const IColumn ** columns, size_t row_num)
    {
        UInt64 hash;

        const IColumn ** column = columns;
        const IColumn ** columns_end = column + num_args;

        {
            StringRef value = (*column)->getDataAt(row_num);
            value = data.getUpdatedValueForCollator(value, 0);
            hash = CityHash_v1_0_2::CityHash64(value.data, value.size);
            ++column;
        }

        while (column < columns_end)
        {
            StringRef value = (*column)->getDataAt(row_num);
            value = data.getUpdatedValueForCollator(value, column - columns);
            hash = CityHash_v1_0_2::Hash128to64(
                CityHash_v1_0_2::uint128(CityHash_v1_0_2::CityHash64(value.data, value.size), hash));
            ++column;
        }

        return hash;
    }
};

template <typename Data>
struct UniqVariadicHash<Data, false, true>
{
    static inline UInt64 apply(Data &, size_t num_args, const IColumn ** columns, size_t row_num)
    {
        UInt64 hash;

        const Columns & tuple_columns = static_cast<const ColumnTuple *>(columns[0])->getColumns();

        const ColumnPtr * column = tuple_columns.data();
        const ColumnPtr * columns_end = column + num_args;

        {
            StringRef value = column->get()->getDataAt(row_num);
            hash = CityHash_v1_0_2::CityHash64(value.data, value.size);
            ++column;
        }

        while (column < columns_end)
        {
            StringRef value = column->get()->getDataAt(row_num);
            hash = CityHash_v1_0_2::Hash128to64(
                CityHash_v1_0_2::uint128(CityHash_v1_0_2::CityHash64(value.data, value.size), hash));
            ++column;
        }

        return hash;
    }
};

template <typename Data>
struct UniqVariadicHash<Data, true, false>
{
    static inline UInt128 apply(Data & data, size_t num_args, const IColumn ** columns, size_t row_num)
    {
        const IColumn ** column = columns;
        const IColumn ** columns_end = column + num_args;

        SipHash hash;

        while (column < columns_end)
        {
            auto collator_and_sort_key_container = data.getCollatorAndSortKeyContainer(column - columns);
            (*column)->updateHashWithValue(
                row_num,
                hash,
                collator_and_sort_key_container.first,
                *collator_and_sort_key_container.second);
            ++column;
        }

        UInt128 key;
        hash.get128(key.low, key.high);
        return key;
    }
};

template <typename Data>
struct UniqVariadicHash<Data, true, true>
{
    static inline UInt128 apply(Data &, size_t num_args, const IColumn ** columns, size_t row_num)
    {
        const Columns & tuple_columns = static_cast<const ColumnTuple *>(columns[0])->getColumns();

        const ColumnPtr * column = tuple_columns.data();
        const ColumnPtr * columns_end = column + num_args;

        SipHash hash;

        while (column < columns_end)
        {
            (*column)->updateHashWithValue(row_num, hash);
            ++column;
        }

        UInt128 key;
        hash.get128(key.low, key.high);
        return key;
    }
};

} // namespace DB

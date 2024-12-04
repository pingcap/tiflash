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

#include <Columns/IColumn.h>


/// Common helper methods for implementation of different columns.

namespace DB
{
/// Counts how many bytes of `filt` are greater than zero.
size_t countBytesInFilter(const UInt8 * filt, size_t sz);
size_t countBytesInFilter(const UInt8 * filt, size_t start, size_t sz);
size_t countBytesInFilter(const IColumn::Filter & filt);
size_t countBytesInFilter(const IColumn::Filter & filt, size_t start, size_t sz);
size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map);
size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map, size_t start, size_t sz);

/// Returns vector with num_columns elements. vector[i] is the count of i values in selector.
/// Selector must contain values from 0 to num_columns - 1. NOTE: this is not checked.
std::vector<size_t> countColumnsSizeInSelector(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector);

/// The general implementation of `filter` function for ColumnArray and ColumnString.
template <typename T>
void filterArraysImpl(
    const PaddedPODArray<T> & src_elems,
    const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    IColumn::Offsets & res_offsets,
    const IColumn::Filter & filt,
    ssize_t result_size_hint);

/// Same as above, but not fills res_offsets.
template <typename T>
void filterArraysImplOnlyData(
    const PaddedPODArray<T> & src_elems,
    const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    const IColumn::Filter & filt,
    ssize_t result_size_hint);

template <typename T, typename Container>
void filterImpl(const UInt8 *& filt_pos, const UInt8 *& filt_end, const T *& data_pos, Container & res_data);

} // namespace DB

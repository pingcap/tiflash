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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/GatherUtils/IArraySink.h>
#include <Functions/GatherUtils/IArraySource.h>
#include <Functions/GatherUtils/IValueSource.h>

#include <type_traits>

/** These methods are intended for implementation of functions, that
  *  copy ranges from one or more columns to another column.
  *
  * Example:
  * - concatenation of strings and arrays (concat);
  * - extracting slices and elements of strings and arrays (substring, arraySlice, arrayElement);
  * - creating arrays from several columns ([x, y]);
  * - conditional selecting from several string or array columns (if, multiIf);
  * - push and pop elements from array front or back (arrayPushBack, etc);
  * - splitting strings into arrays and joining arrays back;
  * - formatting strings (format).
  *
  * There are various Sources, Sinks and Slices.
  * Source - allows to iterate over a column and obtain Slices.
  * Slice - a reference to elements to copy.
  * Sink - allows to build result column by copying Slices into it.
  */

namespace DB::GatherUtils
{
std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows);
std::unique_ptr<IValueSource> createValueSource(const IColumn & col, bool is_const, size_t total_rows);
std::unique_ptr<IArraySink> createArraySink(ColumnArray & col, size_t column_size);

void concat(const std::vector<std::unique_ptr<IArraySource>> & sources, IArraySink & sink);

void sliceFromLeftConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset);

void sliceFromLeftConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length);

void sliceFromRightConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset);

void sliceFromRightConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length);

void sliceDynamicOffsetUnbounded(IArraySource & src, IArraySink & sink, const IColumn & offset_column);

void sliceDynamicOffsetBounded(
    IArraySource & src,
    IArraySink & sink,
    const IColumn & offset_column,
    const IColumn & length_column);

void sliceHas(IArraySource & first, IArraySource & second, bool all, ColumnUInt8 & result);

void push(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, bool push_back);

void resizeDynamicSize(
    IArraySource & array_source,
    IValueSource & value_source,
    IArraySink & sink,
    const IColumn & size_column);

void resizeConstantSize(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, ssize_t size);
} // namespace DB::GatherUtils

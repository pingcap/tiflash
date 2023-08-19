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


namespace DB
{
/// Support methods for implementation of WHERE, PREWHERE and HAVING.


/// Analyze if the column for filter is constant thus filter is always false or always true.
struct ConstantFilterDescription
{
    bool always_false = false;
    bool always_true = false;

    ConstantFilterDescription() = default;
    explicit ConstantFilterDescription(const IColumn & column);
};


/// Obtain a filter from non constant Column, that may have type: UInt8, Nullable(UInt8).
struct FilterDescription
{
    const IColumn::Filter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder; /// If new column was generated, it will be owned by holder.

    explicit FilterDescription(const IColumn & column);
};

} // namespace DB

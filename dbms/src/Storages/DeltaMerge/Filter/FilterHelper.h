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

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB::DM
{

inline RSOperatorPtr toFilter(RowKeyRange & rowkey_range)
{
    Attr handle_attr = {
        EXTRA_HANDLE_COLUMN_NAME,
        EXTRA_HANDLE_COLUMN_ID,
        rowkey_range.is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE,
    };
    if (rowkey_range.is_common_handle)
    {
        auto left = createGreaterEqual(
            handle_attr,
            Field(rowkey_range.start.value->data(), rowkey_range.start.value->size()),
            -1);
        auto right = createLess(handle_attr, Field(rowkey_range.end.value->data(), rowkey_range.end.value->size()), -1);
        return createAnd({left, right});
    }
    else
    {
        auto left = createGreaterEqual(handle_attr, Field(rowkey_range.start.int_value), -1);
        auto right = createLess(handle_attr, Field(rowkey_range.end.int_value), -1);
        return createAnd({left, right});
    }
}

} // namespace DB::DM

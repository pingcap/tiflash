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

#include <Columns/ColumnFunction.h>
#include <Columns/ShortCircuitExecution.h>
#include <Columns/countBytesInFilter.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{
const ColumnFunction * checkAndGetShortCircuitArgument(const ColumnPtr & column)
{
    const auto * function = typeid_cast<const ColumnFunction *>(column.get());
    return function && function->isShortCircuitArgument() ? function : nullptr;
}

void maskedExecute(ColumnWithTypeAndName & column, const IColumn::Filter & mask)
{
    const auto * deferred = checkAndGetShortCircuitArgument(column.column);
    if (!deferred)
        return;

    RUNTIME_CHECK(column.column->size() == mask.size());
    const size_t selected = countBytesInFilter(mask);
    if (selected == 0)
    {
        column.column = column.type->createColumnConstWithDefaultValue(mask.size());
        return;
    }
    if (selected == mask.size())
    {
        column.column = deferred->reduce().column;
        return;
    }

    /// Let the function consume the mask itself (IFunctionBase::
    /// executeWithMask): it decides how to treat the masked-off rows and
    /// returns a full-size column, so no filter/expand copies are needed at
    /// this level.
    column.column = deferred->reduceWithMask(mask);
}

} // namespace DB

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
#include <Core/ColumnWithTypeAndName.h>

namespace DB
{
class ColumnFunction;

/// Returns the column as a ColumnFunction created for a deferred
/// short-circuit argument, or nullptr for a materialized column.
const ColumnFunction * checkAndGetShortCircuitArgument(const ColumnPtr & column);

/// Materialize a deferred (short-circuit argument) column in place for the
/// rows selected by `mask`: the result is a full-size column whose masked-off
/// rows hold values the consumer will never propagate.
void maskedExecute(ColumnWithTypeAndName & column, const IColumn::Filter & mask);

} // namespace DB

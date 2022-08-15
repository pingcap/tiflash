// Copyright 2022 PingCAP, Ltd.
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
#include <TestUtils/AggFunctionTestUtils.h>
namespace DB::tests
{
ColumnWithTypeAndName AggregateFunctionTest::executeAggregateFunction(const String & func_name,
                                                                      const ColumnsWithTypeAndName & cols,
                                                                      const ColumnNumbers & group_keys_offset,
                                                                      const bool empty_result_for_aggregation_by_empty_set = false,
                                                                      const bool allow_to_use_two_level_group_by = false,
                                                                      const TiDB::TiDBCollators & collators = TiDB::dummy_collators)
{
    MockColumnInfoVec col_infos;
    col_infos.reserve(cols.size());
    for (auto col: cols) 
    {
        col_infos.emplace_back({col.name, col.type})
    }
    context.addMockTable("test", "table", );
    auto request = context.scan(xx, xx).
}
} // namespace DB::tests
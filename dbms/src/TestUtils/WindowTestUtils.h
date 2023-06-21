// Copyright 2023 PingCAP, Ltd.
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

#include <TestUtils/ExecutorTestUtils.h>
#include <tipb/expression.pb.h>
#include <TestUtils/mockExecutor.h>
#include <Debug/MockExecutor/WindowBinder.h>

#include <memory>

namespace DB::tests
{
class WindowTest : public ExecutorTest
{
protected:
// TODO
//   1. different range_val type, and could support Decimal type
//   2. could choose plus or minus function
//   ...
mock::MockWindowFrameBound buildRangeFrameBound(
    tipb::WindowBoundType bound_type_,
    tipb::RangeCmpDataType cmp_data_type_,
    const String & order_by_col_name,
    Int64 range_val)
{
    mock::BuildRangeFrameHelper helper;
    helper.range_aux_func = plusInt(col(order_by_col_name), lit(Field(range_val)));
    helper.context = context.context;
    return mock::MockWindowFrameBound(bound_type_, cmp_data_type_, helper);
}
};
} // namespace DB::tests

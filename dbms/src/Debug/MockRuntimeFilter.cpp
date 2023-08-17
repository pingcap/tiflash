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

#include <Debug/MockRuntimeFilter.h>

namespace DB::mock
{
void MockRuntimeFilter::toPB(
    const DAGSchema & source_schema,
    const DAGSchema & target_schema,
    int32_t collator_id,
    const Context & context,
    tipb::RuntimeFilter * rf)
{
    rf->set_id(id);
    auto * source_expr_pb = rf->mutable_source_expr_list()->Add();
    astToPB(source_schema, source_expr, source_expr_pb, collator_id, context);
    auto * target_expr_pb = rf->mutable_target_expr_list()->Add();
    astToPB(target_schema, target_expr, target_expr_pb, collator_id, context);
    rf->set_source_executor_id(source_executor_id);
    rf->set_target_executor_id(target_executor_id);
    rf->set_rf_type(tipb::IN);
    rf->set_rf_mode(tipb::LOCAL);
}
} // namespace DB::mock

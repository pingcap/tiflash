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

#include <Interpreters/ExpressionActions.h>

namespace DB::PhysicalPlanHelper
{
ExpressionActionsPtr newActions(const Block & input_block);

ExpressionActionsPtr newActions(const NamesAndTypes & input_columns);

NamesAndTypes addSchemaProjectAction(
    const ExpressionActionsPtr & expr_actions,
    const NamesAndTypes & before_schema,
    const String & column_prefix = "");

void addParentRequireProjectAction(const ExpressionActionsPtr & expr_actions, const Names & parent_require);
} // namespace DB::PhysicalPlanHelper

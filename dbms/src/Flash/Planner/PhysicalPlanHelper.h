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

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

namespace DB::PhysicalPlanHelper
{
Names schemaToNames(const NamesAndTypes & schema);

ExpressionActionsPtr newActions(const Block & input_block, const Context & context);
} // namespace DB::PhysicalPlanHelper

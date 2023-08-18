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

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ExpressionActions.h>
#include <common/types.h>

namespace DB::FinalizeHelper
{
void prependProjectInputIfNeed(ExpressionActionsPtr & actions, size_t columns_from_previous);

void checkSchemaContainsParentRequire(const NamesAndTypes & schema, const Names & parent_require);

void checkSampleBlockContainsSchema(const Block & sample_block, const NamesAndTypes & schema);

void checkSampleBlockContainsParentRequire(const Block & sample_block, const Names & parent_require);
} // namespace DB::FinalizeHelper

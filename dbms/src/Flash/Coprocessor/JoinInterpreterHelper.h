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

#include <DataTypes/IDataType.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

#include <tuple>

namespace DB::JoinInterpreterHelper
{
std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join);

DataTypes getJoinKeyTypes(const tipb::Join & join);

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const DataTypes & key_types);
} // namespace DB::JoinInterpreterHelper
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

#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>


namespace DB
{
namespace DM
{
void setColumnDefineDefaultValue(const AlterCommand & command, ColumnDefine & define);
void setColumnDefineDefaultValue(const TiDB::TableInfo & table_info, ColumnDefine & define);
void applyAlter(
    ColumnDefines & table_columns,
    const AlterCommand & command,
    const OptionTableInfoConstRef table_info,
    ColumnID & max_column_id_used);

} // namespace DM
} // namespace DB
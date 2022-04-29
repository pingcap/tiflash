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

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Storages/Transaction/TiDB.h>
#include <tipb/executor.pb.h>

namespace DB
{
using DAGColumnInfo = std::pair<String, TiDB::ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;
NamesAndTypes genNamesAndTypesFromTableScan(const tipb::TableScan & table_scan);
DAGSchema genSchemaFromTableScan(const tipb::TableScan & table_scan);
ColumnsWithTypeAndName getColumnWithTypeAndName(const DAGSchema & schema);
ColumnsWithTypeAndName getColumnWithTypeAndName(const NamesAndTypes & names_and_types);
} // namespace DB
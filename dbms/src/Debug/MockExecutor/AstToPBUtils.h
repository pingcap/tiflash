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

#include <Common/Exception.h>
#include <Poco/StringTokenizer.h>
#include <TiDB/Schema/TiDB.h>
#include <common/types.h>

namespace DB
{

using DAGColumnInfo = std::pair<String, TiDB::ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

// We use qualified format like "db_name.table_name.column_name"
// to identify one column of a table.
// We can split the qualified format into the ColumnName struct.
struct ColumnName
{
    String db_name;
    String table_name;
    String column_name;
};

ColumnName splitQualifiedName(const String & s);

DAGSchema::const_iterator checkSchema(const DAGSchema & input, const String & checked_column);

DAGColumnInfo toNullableDAGColumnInfo(const DAGColumnInfo & input);

namespace Debug
{
extern String LOCAL_HOST;

void setServiceAddr(const std::string & addr);
} // namespace Debug
} // namespace DB

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
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM::tests
{
struct WorkloadOptions;

struct TableInfo
{
    TableID table_id;
    std::string db_name;
    std::string table_name;
    DB::DM::ColumnDefinesPtr columns;
    std::vector<int> rowkey_column_indexes;
    DB::DM::ColumnDefine handle;
    bool is_common_handle;
    std::vector<std::string> toStrings() const;
};

class TableGenerator
{
public:
    static std::unique_ptr<TableGenerator> create(const WorkloadOptions & opts);

    virtual TableInfo get(int64_t table_id, std::string table_name) = 0;

    virtual ~TableGenerator() = default;
};
} // namespace DB::DM::tests
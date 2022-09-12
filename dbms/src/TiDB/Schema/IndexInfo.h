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

#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/SchemaState.h>

namespace TiDB
{
using DB::ColumnID;

struct IndexInfo
{
    IndexInfo() = default;

    explicit IndexInfo(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    Int64 id;
    String idx_name;
    String tbl_name;
    std::vector<IndexColumnInfo> idx_cols;
    SchemaState state;
    Int32 index_type;
    bool is_unique;
    bool is_primary;
    bool is_invisible;
    bool is_global;
};
} // namespace TiDB
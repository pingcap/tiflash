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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MutableSupport.h>

namespace DB
{
const DataTypePtr & MutSup::getExtraHandleColumnIntType()
{
    static const auto type = DataTypeFactory::instance().getOrSet("Int64");
    return type;
}

const DataTypePtr & MutSup::getExtraHandleColumnStringType()
{
    static const auto name_and_types = DataTypeString::getTiDBPkColumnStringNameAndTypes();

    const auto default_name = DataTypeString::getDefaultName();
    auto itr = std::find_if(name_and_types.begin(), name_and_types.end(), [&default_name](const auto & name_and_type) {
        return name_and_type.first == default_name;
    });
    RUNTIME_CHECK_MSG(
        itr != name_and_types.end(),
        "Cannot find '{}' for TiDB primary key column string type",
        default_name);
    return itr->second;
}

const DataTypePtr & MutSup::getVersionColumnType()
{
    static const auto type = DataTypeFactory::instance().getOrSet("UInt64");
    return type;
}

const DataTypePtr & MutSup::getDelmarkColumnType()
{
    static const auto type = DataTypeFactory::instance().getOrSet("UInt8");
    return type;
}

const DataTypePtr & MutSup::getExtraTableIdColumnType()
{
    /// it should not be nullable, but TiDB does not set not null flag for extra_table_id_column_type, so has to align with TiDB
    static const auto type = DataTypeFactory::instance().getOrSet("Nullable(Int64)");
    return type;
}

} // namespace DB

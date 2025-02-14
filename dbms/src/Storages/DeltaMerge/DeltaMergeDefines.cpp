// Copyright 2024 PingCAP, Inc.
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

#include <DataTypes/DataTypeString.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM
{

const ColumnDefine & getExtraIntHandleColumnDefine()
{
    static const ColumnDefine int_handle_cd{
        MutSup::extra_handle_id,
        MutSup::extra_handle_column_name,
        MutSup::getExtraHandleColumnIntType()};
    return int_handle_cd;
}

namespace
{
auto getExtraStringHandleColumnDefines()
{
    const auto names_and_types = ::DB::DataTypeString::getTiDBPkColumnStringNameAndTypes();
    std::vector<std::pair<String, ColumnDefine>> name_and_column_defines;
    name_and_column_defines.reserve(names_and_types.size());
    for (const auto & name_and_type : names_and_types)
    {
        name_and_column_defines.emplace_back(
            name_and_type.first,
            ColumnDefine{MutSup::extra_handle_id, MutSup::extra_handle_column_name, name_and_type.second});
    }
    return name_and_column_defines;
}
} // namespace
const ColumnDefine & getExtraStringHandleColumnDefine()
{
    static const auto name_and_column_defines = getExtraStringHandleColumnDefines();

    const auto default_name = ::DB::DataTypeString::getDefaultName();
    auto itr = std::find_if(
        name_and_column_defines.begin(),
        name_and_column_defines.end(),
        [&default_name](const auto & name_and_column_define) { return name_and_column_define.first == default_name; });
    RUNTIME_CHECK_MSG(
        itr != name_and_column_defines.end(),
        "Cannot find '{}' for TiDB primary key column string type",
        default_name);
    return itr->second;
}

const ColumnDefine & getExtraHandleColumnDefine(bool is_common_handle)
{
    if (is_common_handle)
        return getExtraStringHandleColumnDefine();
    return getExtraIntHandleColumnDefine();
}

const ColumnDefine & getVersionColumnDefine()
{
    static const ColumnDefine version_cd{
        MutSup::version_col_id,
        MutSup::version_column_name,
        MutSup::getVersionColumnType()};
    return version_cd;
}

const ColumnDefine & getTagColumnDefine()
{
    static const ColumnDefine delmark_cd{
        MutSup::delmark_col_id,
        MutSup::delmark_column_name,
        MutSup::getDelmarkColumnType()};
    return delmark_cd;
}

const ColumnDefine & getExtraTableIDColumnDefine()
{
    static const ColumnDefine table_id_cd{
        MutSup::extra_table_id_col_id,
        MutSup::extra_table_id_column_name,
        MutSup::getExtraTableIdColumnType()};
    return table_id_cd;
}
} // namespace DB::DM

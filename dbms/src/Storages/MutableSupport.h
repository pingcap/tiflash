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

#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Storages/KVStore/Types.h>

#include <ext/singleton.h>

namespace DB
{
class MutSup : public ext::Singleton<MutSup>
{
public:
    MutSup()
    {
        mutable_hidden.push_back(version_column_name);
        mutable_hidden.push_back(delmark_column_name);
        //mutable_hidden.push_back(extra_handle_column_name);
    }

    const OrderedNameSet & hiddenColumns(const String & table_type_name)
    {
        if (delta_tree_storage_name == table_type_name)
            return mutable_hidden;
        return empty;
    }

    inline static const String delta_tree_storage_name = "DeltaMerge";

    inline static constexpr ColumnID extra_handle_id = -1;
    inline static constexpr ColumnID extra_table_id_col_id = -3;
    inline static constexpr ColumnID version_col_id = -1024;
    inline static constexpr ColumnID delmark_col_id = -1025;
    inline static constexpr ColumnID invalid_col_id = -10000;

    inline static const String extra_handle_column_name = "_tidb_rowid";
    inline static const String version_column_name = "_INTERNAL_VERSION";
    inline static const String delmark_column_name = "_INTERNAL_DELMARK";
    inline static const String extra_table_id_column_name = "_tidb_tid";

    ALWAYS_INLINE static const DataTypePtr & getExtraHandleColumnIntType();
    ALWAYS_INLINE static const DataTypePtr & getExtraHandleColumnStringType();
    ALWAYS_INLINE static const DataTypePtr & getVersionColumnType();
    ALWAYS_INLINE static const DataTypePtr & getDelmarkColumnType();
    ALWAYS_INLINE static const DataTypePtr & getExtraTableIdColumnType();

private:
    OrderedNameSet empty;
    OrderedNameSet mutable_hidden;
};

} // namespace DB

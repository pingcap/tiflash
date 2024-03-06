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

#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>

#include <ext/singleton.h>

namespace DB
{
class MutableSupport : public ext::Singleton<MutableSupport>
{
public:
    MutableSupport()
    {
        mutable_hidden.push_back(version_column_name);
        mutable_hidden.push_back(delmark_column_name);
        //mutable_hidden.push_back(tidb_pk_column_name);

        all_hidden.insert(all_hidden.end(), mutable_hidden.begin(), mutable_hidden.end());
    }

    const OrderedNameSet & hiddenColumns(const String & table_type_name)
    {
        if (mmt_storage_name == table_type_name || txn_storage_name == table_type_name
            || delta_tree_storage_name == table_type_name)
            return mutable_hidden;
        return empty;
    }

    static const String mmt_storage_name;
    static const String txn_storage_name;
    static const String delta_tree_storage_name;

    static const String tidb_pk_column_name;
    static const String version_column_name;
    static const String delmark_column_name;
    static const String extra_table_id_column_name;

    static const DataTypePtr tidb_pk_column_int_type;
    static const DataTypePtr tidb_pk_column_string_type;
    static const DataTypePtr version_column_type;
    static const DataTypePtr delmark_column_type;
    static const DataTypePtr extra_table_id_column_type;

    /// mark that ColumnId of those columns are defined in dbms/src/Storages/KVStore/Types.h

    // TODO: detail doc about these delete marks
    struct DelMark
    {
        static const UInt8 NONE = 0x00;
        static const UInt8 INTERNAL_DEL = 0x01;
        static const UInt8 DEFINITE_DEL = (INTERNAL_DEL << 1);
        static const UInt8 DEL_MASK = (INTERNAL_DEL | DEFINITE_DEL);
        static const UInt8 DATA_MASK = ~DEL_MASK;

        static UInt8 getData(UInt8 raw_data) { return raw_data & DATA_MASK; }

        static bool isDel(UInt8 raw_data) { return raw_data & DEL_MASK; }

        static bool isDefiniteDel(UInt8 raw_data) { return raw_data & DEFINITE_DEL; }

        static UInt8 genDelMark(bool internal_del, bool definite_del, UInt8 src_data)
        {
            return (internal_del ? INTERNAL_DEL : NONE) | (definite_del ? DEFINITE_DEL : NONE) | getData(src_data);
        }

        static UInt8 genDelMark(bool internal_del, bool definite_del = false)
        {
            return genDelMark(internal_del, definite_del, 0);
        }
    };

private:
    OrderedNameSet empty;
    OrderedNameSet mutable_hidden;
    OrderedNameSet all_hidden;
};

} // namespace DB

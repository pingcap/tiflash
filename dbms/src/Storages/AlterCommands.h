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

#include <Core/NamesAndTypes.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KVStore/Types.h>

namespace DB
{

/// Operation from the ALTER query (except for manipulation with PART/PARTITION). Adding Nested columns is not expanded to add individual columns.
struct AlterCommand
{
    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        MODIFY_PRIMARY_KEY,
        // Rename column is only for tmt/dm schema sync.
        RENAME_COLUMN,
        TOMBSTONE,
        RECOVER,
    };

    Type type;

    String column_name;

    /// Only for DM identify column by column_id but not column_name
    ColumnID column_id;

    /// Only for Rename column.
    String new_column_name;

    /// For DROP COLUMN ... FROM PARTITION
    String partition_name;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};

    /// For ADD - after which column to add a new one. If an empty string, add to the end. To add to the beginning now it is impossible.
    String after_column;

    /// For MODIFY_PRIMARY_KEY
    ASTPtr primary_key;

    /// For TOMBSTONE - timestamp when this table is dropped.
    Timestamp tombstone;

    /// the names are the same if they match the whole name or name_without_dot matches the part of the name up to the dot
    static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
    {
        String name_with_dot = name_without_dot + ".";
        return (
            name_with_dot == name_type.name.substr(0, name_without_dot.length() + 1)
            || name_without_dot == name_type.name);
    }

    /// For MODIFY_COLUMN
    /// find column from `columns` or throw exception
    NamesAndTypesList::Iterator findColumn(NamesAndTypesList & columns) const;

    void apply(ColumnsDescription & columns_description) const;

    AlterCommand() = default;
    AlterCommand(
        const Type type,
        const String & column_name,
        const DataTypePtr & data_type,
        const ColumnDefaultKind default_kind,
        const ASTPtr & default_expression,
        const String & after_column = String{})
        : type{type}
        , column_name{column_name}
        , data_type{data_type}
        , default_kind{default_kind}
        , default_expression{default_expression}
        , after_column{after_column}
    {}
};

class IStorage;
class Context;

class AlterCommands : public std::vector<AlterCommand>
{
public:
    void apply(ColumnsDescription & columns_description) const;

    void validate(IStorage * table, const Context & context);
};

} // namespace DB

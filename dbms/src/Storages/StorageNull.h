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
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull
    : public ext::SharedPtrHelper<StorageNull>
    , public IStorage
{
public:
    std::string getName() const override { return "Null"; }
    std::string getTableName() const override { return table_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context &,
        QueryProcessingStage::Enum &,
        size_t,
        unsigned) override
    {
        return {std::make_shared<NullBlockInputStream>(getSampleBlockForColumns(column_names))};
    }

    BlockOutputStreamPtr write(const ASTPtr &, const Settings &) override
    {
        return std::make_shared<NullBlockOutputStream>(getSampleBlock());
    }

    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name)
        override
    {
        table_name = new_table_name;
    }

    void alter(
        const TableLockHolder &,
        const AlterCommands & params,
        const String & database_name,
        const String & table_name,
        const Context & context) override;

private:
    String table_name;

protected:
    StorageNull(String table_name_, ColumnsDescription columns_description_)
        : IStorage{std::move(columns_description_)}
        , table_name(std::move(table_name_))
    {}
};

} // namespace DB

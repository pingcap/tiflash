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

#include <Common/OptimizedRegularExpression.h>
#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
/** A table that represents the union of an arbitrary number of other tables.
  * All tables must have the same structure.
  */
class StorageMerge : public ext::SharedPtrHelper<StorageMerge>
    , public IStorage
{
public:
    std::string getName() const override { return "Merge"; }
    std::string getTableName() const override { return name; }

    bool isRemote() const override;

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void drop() override {}
    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override { name = new_table_name; }

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(const TableLockHolder &, const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const override;

private:
    String name;
    String source_database;
    OptimizedRegularExpression table_name_regexp;
    const Context & context;

    using StorageListWithLocks = std::list<std::pair<StoragePtr, TableLockHolder>>;

    StorageListWithLocks getSelectedTables() const;

    static Block getBlockWithVirtualColumns(const StorageListWithLocks & selected_tables);

protected:
    StorageMerge(
        const std::string & name_,
        const ColumnsDescription & columns_,
        const String & source_database_,
        const String & table_name_regexp_,
        const Context & context_);
};

} // namespace DB

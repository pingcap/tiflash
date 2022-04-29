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

#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class StorageMaterializedView : public ext::SharedPtrHelper<StorageMaterializedView>
    , public IStorage
{
public:
    std::string getName() const override { return "MaterializedView"; }
    std::string getTableName() const override { return table_name; }
    ASTPtr getInnerQuery() const { return inner_query->clone(); };

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const override { return getTargetTable()->mayBenefitFromIndexForIn(left_in_operand); }

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;
    void drop() override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context) override;
    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context) override;
    void attachPartition(const ASTPtr & partition, bool part, const Context & context) override;
    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context) override;

    void shutdown() override;
    bool checkTableCanBeDropped() const override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    String getDataPath() const override { return getTargetTable()->getDataPath(); }

private:
    String select_database_name;
    String select_table_name;
    String target_database_name;
    String target_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;
    Context & global_context;
    bool has_inner_table = false;

    StoragePtr getTargetTable() const;

    void checkStatementCanBeForwarded() const;

protected:
    StorageMaterializedView(
        const String & table_name_,
        const String & database_name_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);
};

} // namespace DB

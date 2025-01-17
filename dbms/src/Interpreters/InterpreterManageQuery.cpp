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

#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterManageQuery.h>
#include <Parsers/ASTManageQuery.h>
#include <Storages/IStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>

namespace DB
{
BlockIO InterpreterManageQuery::execute()
{
    const ASTManageQuery & ast = typeid_cast<const ASTManageQuery &>(*query_ptr);

    StoragePtr table = context.getTable(ast.database, ast.table);
    StorageDeltaMerge * manageable_storage;
    if (table->getName() == MutSup::delta_tree_storage_name)
    {
        manageable_storage = &dynamic_cast<StorageDeltaMerge &>(*table);
    }
    else
    {
        throw Exception("Manage operation can only be applied to DeltaTree engine tables");
    }

    switch (ast.operation)
    {
    case ManageOperation::Enum::Flush:
    {
        manageable_storage->flushCache(context);
        return {};
    }
    case ManageOperation::Enum::Status:
    {
        BlockIO res;
        res.in = manageable_storage->status();
        return res;
    }
    case ManageOperation::Enum::Check:
    {
        manageable_storage->checkStatus(context);
        return {};
    }
    case ManageOperation::Enum::DeleteRows:
    {
        manageable_storage->deleteRows(context, ast.rows);
        return {};
    }
    case ManageOperation::Enum::MergeDelta:
    {
        manageable_storage->mergeDelta(context);
        return {};
    }
    }
    return {};
}
} // namespace DB

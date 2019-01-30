#include <Interpreters/Context.h>
#include <Interpreters/InterpreterManageQuery.h>
#include <Parsers/ASTManageQuery.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageDeltaMerge.h>

namespace DB
{
BlockIO InterpreterManageQuery::execute()
{
    const ASTManageQuery & ast = typeid_cast<const ASTManageQuery &>(*query_ptr);

    StoragePtr table = context.getTable(ast.database, ast.table);
    if (table->getName() != "DeltaMerge")
    {
        throw Exception("Manage operation can only be applied to DeltaMerge engine tables");
    }
    auto & dm_table = static_cast<StorageDeltaMerge &>(*table);
    switch (ast.operation)
    {
        case ManageOperation::Enum::Flush:
        {
            dm_table.flushDelta();
            return {};
        }
        case ManageOperation::Enum::Status:
        {
            BlockIO res;
            res.in = dm_table.status();
            return res;
        }
        case ManageOperation::Enum::Check:
        {
            dm_table.check();
            return {};
        }
    }
    return {};
}
}

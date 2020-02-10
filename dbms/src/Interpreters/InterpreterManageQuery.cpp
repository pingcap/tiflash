#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterManageQuery.h>
#include <Parsers/ASTManageQuery.h>
#include <Storages/IStorage.h>

#include <Storages/StorageDeltaMerge.h>

namespace DB
{
BlockIO InterpreterManageQuery::execute()
{
    const ASTManageQuery & ast = typeid_cast<const ASTManageQuery &>(*query_ptr);

    StoragePtr table = context.getTable(ast.database, ast.table);
    IManageableStorage * manageable_storage;
    if (table->getName() == "DeltaMerge")
    {
        manageable_storage = &dynamic_cast<StorageDeltaMerge &>(*table);
    }
    else
    {
        throw Exception("Manage operation can only be applied to DeltaMerge engine tables");
    }

    switch (ast.operation)
    {
        case ManageOperation::Enum::Flush:
        {
            manageable_storage->flushDelta();
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
        case ManageOperation::Enum ::DeleteRows:
        {
            manageable_storage->deleteRows(context, ast.rows);
            return {};
        }
    }
    return {};
}
} // namespace DB

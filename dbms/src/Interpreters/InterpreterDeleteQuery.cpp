#include <IO/ConcatReadBuffer.h>

#include <Common/typeid_cast.h>

#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDeleteQuery.h>

#include <Storages/MutableSupport.h>

#include <Interpreters/InterpreterDeleteQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>

namespace ProfileEvents
{
    extern const Event DeleteQuery;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int READONLY;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}

InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, const Context & context_, bool allow_materialized_)
    : query_ptr(query_ptr_), context(context_), allow_materialized(allow_materialized_)
{
    ProfileEvents::increment(ProfileEvents::DeleteQuery);
}

BlockIO InterpreterDeleteQuery::execute()
{
    ASTDeleteQuery & query = typeid_cast<ASTDeleteQuery &>(*query_ptr);
    checkAccess(query);

    StoragePtr table = context.getTable(query.database, query.table);
    if (!table->supportsModification())
       throw Exception("Table engine " + table->getName() + " does not support Delete.");

    auto table_lock = table->lockStructureForShare(context.getCurrentQueryId());

    NamesAndTypesList required_columns = table->getColumns().getAllPhysical();

    BlockOutputStreamPtr out;

    out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, table, context, query_ptr, false);

    out = std::make_shared<AddingDefaultBlockOutputStream>(
        out, table->getSampleBlockNoHidden(), required_columns, table->getColumns().defaults, context);

    out = std::make_shared<SquashingBlockOutputStream>(
        out, context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);

    auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
    out_wrapper->setProcessListElement(context.getProcessListElement());
    out = std::move(out_wrapper);

    BlockIO res;
    res.out = std::move(out);

    if (!query.where)
        throw Exception("Delete query must have WHERE.", ErrorCodes::LOGICAL_ERROR);

    InterpreterSelectQuery interpreter_select(query.select, context);

    res.in = interpreter_select.execute().in;

    res.in = std::make_shared<ConvertingBlockInputStream>(context, res.in, res.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Position);
    res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, res.out);

    res.out = nullptr;

    if (!allow_materialized)
    {
        Block in_header = res.in->getHeader();
        for (const auto & name_type : table->getColumns().materialized)
            if (in_header.has(name_type.name))
                throw Exception("Cannot insert column " + name_type.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
    }

    return res;
}

void InterpreterDeleteQuery::checkAccess(const ASTDeleteQuery & query)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;

    if (!readonly || (query.database.empty() && context.tryGetExternalTable(query.table) && readonly >= 2))
    {
        return;
    }

    throw Exception("Cannot insert into table in readonly mode", ErrorCodes::READONLY);
}

}

#include <gperftools/malloc_extension.h>

#include <Storages/DeltaMerge/DeltaMergeBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeBlockOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTinyLog.h>

#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <Core/Defines.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>

namespace DB
{
StorageDeltaMerge::StorageDeltaMerge(const std::string & path_,
    const std::string & name_,
    const ColumnsDescription & columns_,
    const ASTPtr & primary_expr_ast_,
    bool attach,
    size_t max_compress_block_size_)
    : IStorage{columns_}
    , path(path_)
    , name(name_)
    , max_compress_block_size(max_compress_block_size_)
    , stable_storage(StorageTinyLog::create(path_, name_, columns_, attach, max_compress_block_size_))
    , log(&Logger::get("StorageDeltaMerge"))
{
    primary_sort_descr.reserve(primary_expr_ast_->children.size());
    for (const ASTPtr & ast : primary_expr_ast_->children)
    {
        String name = ast->getColumnName();
        primary_sort_descr.emplace_back(name, 1, 1);
    }

    initDelta();
}

void StorageDeltaMerge::initDelta()
{
    insert_value_space = std::make_shared<MemoryValueSpace>(name, stable_storage->getColumns().getAllPhysical(), primary_sort_descr);
    modify_value_space = std::make_shared<MemoryValueSpace>(name, stable_storage->getColumns().getAllPhysical(), primary_sort_descr);
    delta_tree = std::make_shared<DefaultDeltaTree>(insert_value_space, modify_value_space);

    // Force tcmalloc to return memory back to system.
    // https://internal.pingcap.net/jira/browse/FLASH-41
    MallocExtension::instance()->ReleaseFreeMemory(); 
}

BlockInputStreams StorageDeltaMerge::read( //
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    return read(column_names, processed_stage, max_block_size, context.getSettingsRef().max_read_buffer_size);
}

BlockInputStreams StorageDeltaMerge::read( //
    const Names & column_names,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const size_t max_read_buffer_size)
{
    auto stable_input_stream_ptr
        = static_cast<StorageTinyLog &>(*stable_storage).read(column_names, processed_stage, max_block_size, max_read_buffer_size).at(0);
    auto delta_merge_stream_ptr = std::make_shared<DeltaMergeBlockInputStream>(stable_input_stream_ptr, delta_tree, max_block_size);
    return {delta_merge_stream_ptr};
}

BlockOutputStreamPtr StorageDeltaMerge::write(const ASTPtr & query, const Settings & settings)
{
    const ASTInsertQuery * insert_query = typeid_cast<const ASTInsertQuery *>(&*query);
    const ASTDeleteQuery * delete_query = typeid_cast<const ASTDeleteQuery *>(&*query);
    Action action;
    if (insert_query)
        action = insert_query->is_upsert ? Action::Upsert : Action::Insert;
    else if (delete_query)
        action = Action::Delete;
    else
        throw Exception("Should be insert or delete query.");

    Names primary_keys;
    for (const auto & s : primary_sort_descr)
        primary_keys.push_back(s.column_name);
    auto max_block_size = settings.max_block_size;

    DeltaMergeBlockOutputStream::InputStreamCreator input_stream_creator = [this, primary_keys, max_block_size]() {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        return this->read(primary_keys, stage, max_block_size, DBMS_DEFAULT_BUFFER_SIZE).at(0);
    };
    DeltaMergeBlockOutputStream::Flusher flusher = [this]() { this->flushDelta(); };

    return std::make_shared<DeltaMergeBlockOutputStream>(input_stream_creator, delta_tree, action, primary_sort_descr, flusher, settings.delta_merge_size);
}

void StorageDeltaMerge::flushDelta()
{
    // We deal with it later.
    //    auto lock = lockForAlter(__PRETTY_FUNCTION__);

    auto new_stable_storage = StorageTinyLog::create(path, name + "_flush_", getColumns(), false, max_compress_block_size);
    auto ouput = new_stable_storage->write({}, {});
    auto header = ouput->getHeader();
    auto stage = QueryProcessingStage::Enum::FetchColumns;
    auto input = this->read(header.getNames(), stage, DEFAULT_BLOCK_SIZE, DBMS_DEFAULT_BUFFER_SIZE).at(0);

    copyData(*input, *ouput, []() { return false; });

    Poco::File(path + escapeForFileName(name)).remove(true);

    stable_storage = new_stable_storage;
    stable_storage->rename(path, "", name);
    initDelta();
}

UInt64 StorageDeltaMerge::stable_size()
{
    auto stage = QueryProcessingStage::Enum::FetchColumns;
    /// TODO: get minimum column
    auto stable_input_stream_ptr = static_cast<StorageTinyLog &>(*stable_storage)
                                       .read({getColumns().getNamesOfPhysical().at(0)}, stage, DEFAULT_BLOCK_SIZE, DBMS_DEFAULT_BUFFER_SIZE)
                                       .at(0);
    stable_input_stream_ptr->readPrefix();
    size_t stable_tuples = 0;
    Block input_block = stable_input_stream_ptr->read();
    while (input_block)
    {
        stable_tuples += input_block.rows();
        input_block = stable_input_stream_ptr->read();
    }
    stable_input_stream_ptr->readSuffix();
    return stable_tuples;
}

std::tuple<UInt64, UInt64, UInt64, UInt64> StorageDeltaMerge::delta_status()
{
    UInt64 entries = 0;
    UInt64 inserts = 0;
    UInt64 deletes = 0;
    UInt64 modifies = 0;
    auto it = delta_tree->begin();
    auto end_it = delta_tree->end();
    for (; it != end_it; ++it)
    {
        ++entries;
        if (it.getType() == DT_INS)
            ++inserts;
        else if (it.getType() == DT_DEL)
            deletes += it.getValue();
        else
            ++modifies;
    }
    return {entries, inserts, deletes, modifies};
}

BlockInputStreamPtr StorageDeltaMerge::status()
{
    Block block;

    block.insert({std::make_shared<DataTypeUInt64>(), "stable_tuples"});
    block.insert({std::make_shared<DataTypeUInt64>(), "delta_entries"});
    block.insert({std::make_shared<DataTypeUInt64>(), "delta_inserts"});
    block.insert({std::make_shared<DataTypeUInt64>(), "delta_deletes"});
    block.insert({std::make_shared<DataTypeUInt64>(), "delta_updates"});

    auto columns = block.mutateColumns();
    auto dt_status = delta_status();
    columns[0]->insert(stable_size());
    columns[1]->insert(std::get<0>(dt_status));
    columns[2]->insert(std::get<1>(dt_status));
    columns[3]->insert(std::get<2>(dt_status));
    columns[4]->insert(std::get<3>(dt_status));

    return std::make_shared<OneBlockInputStream>(block);
}

void StorageDeltaMerge::check()
{
    delta_tree->checkAll();

    auto entry_it = delta_tree->begin();
    auto entry_end = delta_tree->end();
    if (entry_it == entry_end || entry_it.getType() != DT_INS)
        return;

    Ids vs_column_offsets;
    const auto & names_and_types = insert_value_space->namesAndTypes();
    for (const auto & c : primary_sort_descr)
    {
        for (size_t i = 0; i < names_and_types.size(); ++i)
        {
            if (c.column_name == names_and_types[i].name)
                vs_column_offsets.push_back(i);
        }
    }

    auto last_entry = entry_it;
    ++entry_it;

    for (; entry_it != entry_end; ++entry_it)
    {
        if (entry_it.getType() != DT_INS)
            continue;

        auto res = compareTuple(
            insert_value_space, entry_it.getValue(), insert_value_space, last_entry.getValue(), primary_sort_descr, vs_column_offsets);
        if (res <= 0)
            throw Exception("Algorithm broken: tuples should be ordered");
        last_entry = entry_it;
    }
}

static ASTPtr extractKeyExpressionList(IAST & node)
{
    const ASTFunction * expr_func = typeid_cast<const ASTFunction *>(&node);

    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple.
        return expr_func->children.at(0);
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node.ptr());
        return res;
    }
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void registerStorageDeltaMerge(StorageFactory & factory)
{
    factory.registerStorage("DeltaMerge", [](const StorageFactory::Arguments & args) {
        if (args.engine_args.size() > 1)
            throw Exception("Engine DeltaMerge expects only one parameter. e.g. engine = DeltaMerge((a, b))");
        if (args.engine_args.size() < 1)
            throw Exception(
                "Engine DeltaMerge needs primary key. e.g. engine = DeltaMerge((a, b))", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTPtr primary_expr_list = extractKeyExpressionList(*args.engine_args[0]);
        return StorageDeltaMerge::create(args.data_path,
            args.table_name,
            args.columns,
            primary_expr_list,
            args.attach,
            args.context.getSettings().max_compress_block_size);
    });
}

} // namespace DB
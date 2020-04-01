#include <Common/Exception.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDebugging.h>
#include <Storages/StorageFactory.h>
#include <Storages/Transaction/TMTContext.h>

#include <map>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes


class DebuggingBlockInputStream : public IProfilingBlockInputStream
{
public:
    DebuggingBlockInputStream(
        const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_, const StorageDebugging & storage_)
        : column_names(column_names_), begin(begin_), end(end_), it(begin), storage(storage_)
    {}

    String getName() const override { return "Memory"; }

    Block getHeader() const override { return storage.getSampleBlockForColumns(column_names); }

protected:
    Block readImpl() override
    {
        if (it == end)
        {
            return Block();
        }
        else
        {
            Block src = *it;
            Block res;

            /// Add only required columns to `res`.
            for (const auto & column_name : column_names)
                res.insert(src.getByName(column_name));

            ++it;
            return res;
        }
    }

private:
    Names column_names;
    BlocksList::iterator begin;
    BlocksList::iterator end;
    BlocksList::iterator it;
    const StorageDebugging & storage;
};


class DebuggingBlockOutputStream : public IBlockOutputStream
{
public:
    explicit DebuggingBlockOutputStream(StorageDebugging & storage_) : storage(storage_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        storage.check(block, true);
        std::lock_guard<std::mutex> lock(storage.mutex);
        storage.data.push_back(block);
    }

private:
    StorageDebugging & storage;
};


StorageDebugging::StorageDebugging( //
    String database_name_, String table_name_, const ColumnsDescription & columns_,
    std::optional<std::reference_wrapper<const TiDB::TableInfo>> table_info, bool tombstone, const Context & context_, Mode mode_)
    : IManageableStorage{tombstone},
      database_name(std::move(database_name_)),
      table_name(std::move(table_name_)),
      mode(mode_),
      global_context(context_)
{
    if (table_info)
        tidb_table_info = table_info->get();

    ColumnsDescription new_columns(columns_.ordinary, columns_.materialized, columns_.materialized, columns_.defaults);
    new_columns.materialized.emplace_back(MutableSupport::version_column_name, MutableSupport::version_column_type);
    new_columns.materialized.emplace_back(MutableSupport::delmark_column_name, MutableSupport::delmark_column_type);
    setColumns(new_columns);
}


BlockInputStreams StorageDebugging::read(const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    std::lock_guard<std::mutex> lock(mutex);

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    BlockInputStreams res;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        res.push_back(std::make_shared<DebuggingBlockInputStream>(column_names, begin, end, *this));
    }

    return res;
}


BlockOutputStreamPtr StorageDebugging::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    if (mode == Mode::RejectFirstWrite && num_write_called == 0)
    {
        num_write_called++;
        throw Exception("Reject first write for test, engine: " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    num_write_called++;
    return std::make_shared<DebuggingBlockOutputStream>(*this);
}


void StorageDebugging::drop()
{
    std::lock_guard<std::mutex> lock(mutex);
    data.clear();
    shutdown();
}

std::string StorageDebugging::getName() const
{
    switch (mode)
    {
        case Mode::RejectFirstWrite:
            return "BuggyMemory-RejectFirstWrite";
        case Mode::Normal:
        default:
            return "Memory";
    }
}

void StorageDebugging::rename(const String &, const String & new_database_name, const String & new_table_name)
{
    database_name = new_database_name;
    table_name = new_table_name;
}

void StorageDebugging::alterFromTiDB(
    const AlterCommands &, const String &, const TiDB::TableInfo &, const SchemaNameMapper &, const Context &)
{
    throw Exception("Method alterFromTiDB is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

const TiDB::TableInfo & StorageDebugging::getTableInfo() const { return tidb_table_info; }

void StorageDebugging::setTableInfo(const TiDB::TableInfo & table_info) { tidb_table_info = table_info; }

SortDescription StorageDebugging::getPrimarySortDescription() const
{
    throw Exception("Method getPrimarySortDescription is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void StorageDebugging::startup()
{
    TMTContext & tmt = global_context.getTMTContext();
    tmt.getStorages().put(std::static_pointer_cast<StorageDebugging>(shared_from_this()));
}

void StorageDebugging::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;
}

void StorageDebugging::removeFromTMTContext()
{
    // remove this table from TMTContext
    TMTContext & tmt_context = global_context.getTMTContext();
    tmt_context.getStorages().remove(tidb_table_info.id);
    tmt_context.getRegionTable().removeTable(tidb_table_info.id);
}

void registerStorageDebugging(StorageFactory & factory)
{
    factory.registerStorage("Debugging", [](const StorageFactory::Arguments & args) {
        if (args.engine_args.size() > 2)
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        TiDB::TableInfo info;
        // Note: if `table_info_json` is not empty, `table_info` store a ref to `info`
        std::optional<std::reference_wrapper<const TiDB::TableInfo>> table_info = std::nullopt;
        bool tombstone = false;
        if (args.engine_args.size() > 1)
        {
            auto ast = typeid_cast<const ASTLiteral *>(args.engine_args[0].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                const auto table_info_json = safeGet<String>(ast->value);
                if (!table_info_json.empty())
                {
                    info.deserialize(table_info_json);
                    if (unlikely(info.columns.empty()))
                        throw Exception("Engine Debugging table info is invalid. # of columns = 0", ErrorCodes::BAD_ARGUMENTS);
                    table_info = info;
                }
            }
            else
                throw Exception("Engine Debugging table info must be a string", ErrorCodes::BAD_ARGUMENTS);
        }
        if (args.engine_args.size() == 2)
        {
            auto ast = typeid_cast<const ASTLiteral *>(args.engine_args[1].get());
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                tombstone = safeGet<UInt64>(ast->value);
            }
            else
                throw Exception("Engine Debugging tombstone must be a UInt64", ErrorCodes::BAD_ARGUMENTS);
        }

        return StorageDebugging::create(args.database_name, args.table_name, args.columns, table_info, tombstone, args.context,
            StorageDebugging::Mode::RejectFirstWrite);
    });
}

} // namespace DB

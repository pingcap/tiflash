#include <random>

#include <gperftools/malloc_extension.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/isSupportedDataTypeCast.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageDeltaMerge-internal.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTinyLog.h>

#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
namespace ErrorCodes
{
extern const int DIRECTORY_ALREADY_EXISTS;
}

using namespace DM;

StorageDeltaMerge::StorageDeltaMerge(const String & path_,
    const String & db_name_,
    const String & table_name_,
    const OptionTableInfoConstRef table_info_,
    const ColumnsDescription & columns_,
    const ASTPtr & primary_expr_ast_,
    Context & global_context_)
    : IManageableStorage{columns_},
      path(path_ + "/" + table_name_),
      db_name(db_name_),
      table_name(table_name_),
      max_column_id_used(0),
      global_context(global_context_),
      log(&Logger::get("StorageDeltaMerge"))
{
    if (primary_expr_ast_->children.empty())
        throw Exception("No primary key");

    // save schema from TiDB
    if (table_info_)
        tidb_table_info = table_info_->get();

    std::unordered_set<String> pks;
    for (size_t i = 0; i < primary_expr_ast_->children.size(); ++i)
    {
        auto col_name = primary_expr_ast_->children[i]->getColumnName();
        pks.emplace(col_name);
        pk_column_names.emplace_back(col_name);
    }

    ColumnsDescription new_columns(columns_.ordinary, columns_.materialized, columns_.materialized, columns_.defaults);

    size_t pks_combined_bytes = 0;
    auto all_columns = getColumns().getAllPhysical();
    ColumnDefines table_column_defines; // column defines used in DeltaMergeStore
    ColumnDefine handle_column_define;
    for (auto & col : all_columns)
    {
        ColumnDefine column_define(0, col.name, col.type);
        if (table_info_)
        {
            /// If TableInfo from TiDB is not empty, we get column id and default value from TiDB
            auto & columns = table_info_->get().columns;
            column_define.id = table_info_->get().getColumnID(column_define.name);
            auto column
                = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & v) -> bool { return v.id == column_define.id; });

            if (column != columns.end())
                column_define.default_value = column->defaultValueToField();
        }
        else
        {
            // in test cases, we allocate column_id here
            column_define.id = max_column_id_used++;
        }


        if (pks.count(col.name))
        {
            if (!col.type->isValueRepresentedByInteger())
                throw Exception("pk column " + col.name + " is not representable by integer");

            pks_combined_bytes += col.type->getSizeOfValueInMemory();
            if (pks_combined_bytes > sizeof(Handle))
                throw Exception("pk columns exceeds size limit :" + DB::toString(sizeof(Handle)));

            if (pks.size() == 1)
                handle_column_define = column_define;
        }

        table_column_defines.push_back(column_define);
    }

    hidden_columns.emplace_back(VERSION_COLUMN_NAME);
    hidden_columns.emplace_back(TAG_COLUMN_NAME);
    new_columns.materialized.emplace_back(VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);
    new_columns.materialized.emplace_back(TAG_COLUMN_NAME, TAG_COLUMN_TYPE);

    if (pks.size() > 1)
    {
        handle_column_define.id = EXTRA_HANDLE_COLUMN_ID;
        handle_column_define.name = EXTRA_HANDLE_COLUMN_NAME;
        handle_column_define.type = EXTRA_HANDLE_COLUMN_TYPE;

        hidden_columns.emplace_back(EXTRA_HANDLE_COLUMN_NAME);
        new_columns.materialized.emplace_back(EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_TYPE);
    }

    setColumns(new_columns);

    assert(!handle_column_define.name.empty());
    assert(!table_column_defines.empty());
    store = std::make_shared<DeltaMergeStore>(global_context, path, db_name, table_name, std::move(table_column_defines),
        std::move(handle_column_define), DeltaMergeStore::Settings());
}

void StorageDeltaMerge::drop()
{
    shutdown();
    // Reclaim memory.
    MallocExtension::instance()->ReleaseFreeMemory();
}

Block StorageDeltaMerge::buildInsertBlock(bool is_import, const Block & old_block)
{
    Block block = old_block;

    if (!is_import)
    {
        // Remove the default columns generated by InterpreterInsertQuery
        if (block.has(EXTRA_HANDLE_COLUMN_NAME))
            block.erase(EXTRA_HANDLE_COLUMN_NAME);
        if (block.has(VERSION_COLUMN_NAME))
            block.erase(VERSION_COLUMN_NAME);
        if (block.has(TAG_COLUMN_NAME))
            block.erase(TAG_COLUMN_NAME);
    }

    const size_t rows = block.rows();
    if (!block.has(store->getHandle().name))
    {
        // put handle column.

        auto handle_column = store->getHandle().type->createColumn();
        auto & handle_data = typeid_cast<ColumnVector<Handle> &>(*handle_column).getData();
        handle_data.resize(rows);

        size_t pk_column_count = pk_column_names.size();
        Columns pk_columns;
        std::vector<DataTypePtr> pk_column_types;
        for (auto & n : pk_column_names)
        {
            auto & col = block.getByName(n);
            pk_columns.push_back(col.column);
            pk_column_types.push_back(col.type);
        }

        for (size_t c = 0; c < pk_column_count; ++c)
        {
            appendIntoHandleColumn(handle_data, pk_column_types[c], pk_columns[c]);
        }

        addColumnToBlock(block, EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_TYPE, std::move(handle_column));
    }

    // add version column
    if (!block.has(VERSION_COLUMN_NAME))
    {
        auto column = VERSION_COLUMN_TYPE->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt64> &>(*column).getData();
        column_data.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = next_version++;
        }

        addColumnToBlock(block, VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE, std::move(column));
    }

    // add tag column (upsert / delete)
    if (!block.has(TAG_COLUMN_NAME))
    {
        auto column = TAG_COLUMN_TYPE->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt8> &>(*column).getData();
        column_data.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = 0;
        }

        addColumnToBlock(block, TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE, std::move(column));
    }

    // Set the real column id.
    const Block & header = store->getHeader();
    for (auto & col : block)
    {
        if (col.name != VERSION_COLUMN_NAME && col.name != TAG_COLUMN_NAME && col.name != EXTRA_HANDLE_COLUMN_NAME)
            col.column_id = header.getByName(col.name).column_id;
    }

    return block;
}

using BlockDecorator = std::function<Block(const Block &)>;
class DMBlockOutputStream : public IBlockOutputStream
{
public:
    DMBlockOutputStream(
        const DeltaMergeStorePtr & store_, const BlockDecorator & decorator_, const Context & db_context_, const Settings & db_settings_)
        : store(store_), header(store->getHeader()), decorator(decorator_), db_context(db_context_), db_settings(db_settings_)
    {}

    Block getHeader() const override { return header; }

    void write(const Block & block) override
    {
        if (db_settings.dm_insert_max_rows == 0)
        {
            store->write(db_context, db_settings, decorator(block));
        }
        else
        {
            Block new_block = decorator(block);
            auto rows = new_block.rows();
            size_t step = db_settings.dm_insert_max_rows;

            for (size_t offset = 0; offset < rows; offset += step)
            {
                size_t limit = std::min(offset + step, rows) - offset;
                Block write_block;
                for (auto & column : new_block)
                {
                    auto col = column.type->createColumn();
                    col->insertRangeFrom(*column.column, offset, limit);
                    write_block.insert(ColumnWithTypeAndName(std::move(col), column.type, column.name, column.column_id));
                }

                store->write(db_context, db_settings, write_block);
            }
        }
    }

private:
    DeltaMergeStorePtr store;
    Block header;
    BlockDecorator decorator;
    const Context & db_context;
    const Settings & db_settings;
};

BlockOutputStreamPtr StorageDeltaMerge::write(const ASTPtr & query, const Settings & settings)
{
    auto & insert_query = typeid_cast<const ASTInsertQuery &>(*query);
    BlockDecorator decorator = std::bind(&StorageDeltaMerge::buildInsertBlock, this, insert_query.is_import, std::placeholders::_1);
    return std::make_shared<DMBlockOutputStream>(store, decorator, global_context, settings);
}


namespace
{

void throwRetryRegion(const MvccQueryInfo::RegionsQueryInfo & regions_info, RegionTable::RegionReadStatus status)
{
    std::vector<RegionID> region_ids;
    region_ids.reserve(regions_info.size());
    for (const auto & info : regions_info)
        region_ids.push_back(info.region_id);
    throw RegionException(std::move(region_ids), status);
}

inline void doLearnerRead(const TiDB::TableID table_id,         //
    const MvccQueryInfo::RegionsQueryInfo & regions_query_info, //
    TMTContext & tmt, Poco::Logger * log)
{
    assert(log != nullptr);

    MvccQueryInfo::RegionsQueryInfo regions_info;
    if (!regions_query_info.empty())
    {
        regions_info = regions_query_info;
    }
    else
    {
        // Only for test, because regions_query_info should never be empty if query is from TiDB or TiSpark.
        auto regions = tmt.getRegionTable().getRegionsByTable(table_id);
        regions_info.reserve(regions.size());
        for (const auto & [id, region] : regions)
        {
            if (region == nullptr)
                continue;
            regions_info.emplace_back(RegionQueryInfo{id, region->version(), region->confVer(), {0, 0}});
        }
    }

    KVStorePtr & kvstore = tmt.getKVStore();
    RegionMap kvstore_region;
    // check region is not null and store region map.
    for (const auto & info : regions_info)
    {
        auto region = kvstore->getRegion(info.region_id);
        if (region == nullptr)
        {
            LOG_WARNING(log, "[region " << info.region_id << "] is not found in KVStore, try again");
            throwRetryRegion(regions_info, RegionTable::RegionReadStatus::NOT_FOUND);
        }
        kvstore_region.emplace(info.region_id, std::move(region));
    }
    // make sure regions are not duplicated.
    if (unlikely(kvstore_region.size() != regions_info.size()))
        throw Exception("Duplicate region id", ErrorCodes::LOGICAL_ERROR);

    auto start_time = Clock::now();
    /// Blocking learner read. Note that learner read must be performed ahead of data read,
    /// otherwise the desired index will be blocked by the lock of data read.
    for (auto && [region_id, region] : kvstore_region)
    {
        (void)region_id;
        region->waitIndex(region->learnerRead());
    }
    auto end_time = Clock::now();
    LOG_DEBUG(log,
        "[Learner Read] wait index cost " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms");

    /// If `disable_bg_flush` is true, we don't need to do both flushing KVStore or waiting for background tasks.
    if (!tmt.disableBgFlush())
    {
        // After raft index is satisfied, we flush region to StorageDeltaMerge so that we can read all data
        start_time = Clock::now();
        std::set<RegionID> regions_flushing_in_bg_threads;
        auto & region_table = tmt.getRegionTable();
        for (auto && [region_id, region] : kvstore_region)
        {
            (void)region;
            bool is_flushed = region_table.tryFlushRegion(region_id, table_id, false);
            // If region is flushing by other bg threads, we should mark those regions to wait.
            if (!is_flushed)
            {
                regions_flushing_in_bg_threads.insert(region_id);
                LOG_DEBUG(log, "[Learner Read] region " << region_id << " is flushing by other thread.");
            }
        }
        end_time = Clock::now();
        LOG_DEBUG(log,
            "[Learner Read] flush " << kvstore_region.size() - regions_flushing_in_bg_threads.size() << " regions of "
                                    << kvstore_region.size() << " to StorageDeltaMerge cost "
                                    << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms");

        // Maybe there is some data not flush to store yet, we should wait till all regions is flushed.
        if (!regions_flushing_in_bg_threads.empty())
        {
            start_time = Clock::now();
            for (const auto & region_id : regions_flushing_in_bg_threads)
            {
                region_table.waitTillRegionFlushed(region_id);
            }
            end_time = Clock::now();
            LOG_DEBUG(log,
                "[Learner Read] wait bg flush " << regions_flushing_in_bg_threads.size() << " regions to StorageDeltaMerge cost "
                                                << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count()
                                                << " ms");
        }
    }
}

} // namespace

BlockInputStreams StorageDeltaMerge::read( //
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    ColumnDefines to_read;
    const Block & header = store->getHeader();
    for (auto & n : column_names)
    {
        ColumnDefine col_define;
        if (n == EXTRA_HANDLE_COLUMN_NAME)
            col_define = store->getHandle();
        else if (n == VERSION_COLUMN_NAME)
            col_define = getVersionColumnDefine();
        else if (n == TAG_COLUMN_NAME)
            col_define = getTagColumnDefine();
        else
        {
            auto & column = header.getByName(n);
            col_define.name = column.name;
            col_define.id = column.column_id;
            col_define.type = column.type;
            col_define.default_value = column.default_value;
        }
        to_read.push_back(col_define);
    }


    const ASTSelectQuery & select_query = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    if (select_query.raw_for_mutable)
        return store->readRaw(context, context.getSettingsRef(), to_read, num_streams);
    else
    {
        if (unlikely(!query_info.mvcc_query_info))
            throw Exception("mvcc query info is null", ErrorCodes::LOGICAL_ERROR);

        TMTContext & tmt = context.getTMTContext();
        if (unlikely(!tmt.isInitialized()))
            throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

        const auto & mvcc_query_info = *query_info.mvcc_query_info;
        // Read with specify tso, check if tso is smaller than TiDB GcSafePoint
        const auto safe_point = tmt.getPDClient()->getGCSafePoint();
        if (mvcc_query_info.read_tso < safe_point)
            throw Exception("query id: " + context.getCurrentQueryId() + ", read tso: " + toString(mvcc_query_info.read_tso)
                    + " is smaller than tidb gc safe point: " + toString(safe_point),
                ErrorCodes::LOGICAL_ERROR);

        // With `no_kvstore` is true, we do not do learner read
        if (likely(!select_query.no_kvstore))
        {
            /// Learner read.
            doLearnerRead(tidb_table_info.id, mvcc_query_info.regions_query_info, tmt, log);

            if (likely(!mvcc_query_info.regions_query_info.empty()))
            {
                /// For learner read from TiDB/TiSpark, we set num_streams by `mvcc_query_info.concurrent`
                num_streams = std::max(1U, static_cast<UInt32>(mvcc_query_info.concurrent));
            } // else learner read from ch-client, keep num_streams
        }

        HandleRanges ranges = getQueryRanges(mvcc_query_info.regions_query_info);

        if (log->trace())
        {
            {
                std::stringstream ss;
                for (const auto & region : mvcc_query_info.regions_query_info)
                {
                    const auto & range = region.range_in_table;
                    ss << region.region_id << "[" << range.first.toString() << "," << range.second.toString() << "),";
                }
                LOG_TRACE(log, "reading ranges: orig: " << ss.str());
            }
            {
                std::stringstream ss;
                for (const auto & range : ranges)
                    ss << range.toString() << ",";
                LOG_TRACE(log, "reading ranges: " << ss.str());
            }
        }

        return store->read(
            context, context.getSettingsRef(), to_read, ranges, num_streams, /*max_version=*/mvcc_query_info.read_tso, max_block_size);
    }
}

void StorageDeltaMerge::checkStatus(const Context & context) { store->check(context); }

void StorageDeltaMerge::deleteRows(const Context & context, size_t rows)
{
    size_t total_rows = 0;

    {
        ColumnDefines to_read{getExtraHandleColumnDefine()};
        auto stream = store->read(context, context.getSettingsRef(), to_read, {DM::HandleRange::newAll()}, 1, MAX_UINT64)[0];
        stream->readPrefix();
        Block block;
        while ((block = stream->read()))
            total_rows += block.rows();
        stream->readSuffix();
    }

    rows = std::min(total_rows, rows);
    auto start_index = rand() % (total_rows - rows + 1);

    DM::HandleRange range = DM::HandleRange::newAll();
    {
        ColumnDefines to_read{getExtraHandleColumnDefine()};
        auto stream = store->read(context, context.getSettingsRef(), to_read, {DM::HandleRange::newAll()}, 1, MAX_UINT64)[0];
        stream->readPrefix();
        Block block;
        size_t index = 0;
        while ((block = stream->read()))
        {
            auto & data = toColumnVectorData<Handle>(block.getByPosition(0).column);
            for (size_t i = 0; i < data.size(); ++i)
            {
                if (index == start_index)
                    range.start = data[i];
                if (index == start_index + rows)
                    range.end = data[i];
                ++index;
            }
        }
        stream->readSuffix();
    }

    store->deleteRange(context, context.getSettingsRef(), range);
}

//==========================================================================================
// DDL methods.
//==========================================================================================
void StorageDeltaMerge::alterFromTiDB(
    const AlterCommands & params, const TiDB::TableInfo & table_info, const String & database_name, const Context & context)
{
    tidb_table_info = table_info;
    alterImpl(params, database_name, table_info.name, std::optional<std::reference_wrapper<const TiDB::TableInfo>>(table_info), context);
}

void StorageDeltaMerge::alter(
    const AlterCommands & commands, const String & database_name, const String & table_name_, const Context & context)
{
    alterImpl(commands, database_name, table_name_, std::nullopt, context);
}

/// If any ddl statement change StorageDeltaMerge's schema,
/// we need to update the create statement in metadata, so that we can restore table structure next time
static void updateDeltaMergeTableCreateStatement(            //
    const String & database_name, const String & table_name, //
    const ColumnsDescription & columns,
    const OrderedNameSet & hidden_columns,                                                         //
    const OptionTableInfoConstRef table_info_from_tidb, const ColumnDefines & store_table_columns, //
    const Context & context);

void StorageDeltaMerge::alterImpl(const AlterCommands & commands,
    const String & database_name,
    const String & table_name_,
    const OptionTableInfoConstRef table_info,
    const Context & context)
{
    std::unordered_set<String> cols_drop_forbidden;
    for (const auto & n : pk_column_names)
        cols_drop_forbidden.insert(n);
    cols_drop_forbidden.insert(EXTRA_HANDLE_COLUMN_NAME);
    cols_drop_forbidden.insert(VERSION_COLUMN_NAME);
    cols_drop_forbidden.insert(TAG_COLUMN_NAME);

    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_PRIMARY_KEY)
        {
            // check that add primary key is forbidden
            throw Exception("Storage engine " + getName() + " doesn't support modify primary key.", ErrorCodes::BAD_ARGUMENTS);
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            // check that drop primary key is forbidden
            // check that drop hidden columns is forbidden
            if (cols_drop_forbidden.count(command.column_name) > 0)
                throw Exception("Storage engine " + getName() + " doesn't support drop primary key / hidden column: " + command.column_name,
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    auto table_soft_lock = lockDataForAlter(__PRETTY_FUNCTION__);
    auto table_hard_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    // update the metadata in database, so that we can read the new schema using TiFlash's client
    ColumnsDescription new_columns = getColumns();

    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            // find the column we are going to modify
            auto col_iter = command.findColumn(new_columns.ordinary); // just find in ordinary columns
            if (!isSupportedDataTypeCast(col_iter->type, command.data_type))
            {
                // check that lossy changes is forbidden
                // check that changing the UNSIGNED attribute is forbidden
                throw Exception("Storage engine " + getName() + "doesn't support lossy data type modify from " + col_iter->type->getName()
                        + " to " + command.data_type->getName(),
                    ErrorCodes::NOT_IMPLEMENTED);
            }
        }
    }

    commands.apply(new_columns); // apply AlterCommands to `new_columns`
    // apply alter to store's table column in DeltaMergeStore
    store->applyAlters(commands, table_info, max_column_id_used, context);
    // after update `new_columns` and store's table columns, we need to update create table statement,
    // so that we can restore table next time.
    updateDeltaMergeTableCreateStatement(
        database_name, table_name_, new_columns, hidden_columns, table_info, store->getTableColumns(), context);
    setColumns(std::move(new_columns));
}

void StorageDeltaMerge::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
    const String new_path = new_path_to_db + "/" + new_table_name;

    if (Poco::File{new_path}.exists())
        throw Exception{"Target path already exists: " + new_path,
            /// @todo existing target can also be a file, not directory
            ErrorCodes::DIRECTORY_ALREADY_EXISTS};

    // flush store and then reset store to new path
    store->flushCache(global_context);
    ColumnDefines table_column_defines = store->getTableColumns();
    ColumnDefine handle_column_define = store->getHandle();
    DeltaMergeStore::Settings settings = store->getSettings();

    store = {};

    // rename path and generate a new store
    Poco::File(path).renameTo(new_path);
    store = std::make_shared<DeltaMergeStore>(global_context, //
        new_path, new_database_name, new_table_name,          //
        std::move(table_column_defines), std::move(handle_column_define), settings);

    path = new_path;
    db_name = new_database_name;
    table_name = new_table_name;
}

void updateDeltaMergeTableCreateStatement(                   //
    const String & database_name, const String & table_name, //
    const ColumnsDescription & columns,
    const OrderedNameSet & hidden_columns,                                                         //
    const OptionTableInfoConstRef table_info_from_tidb, const ColumnDefines & store_table_columns, //
    const Context & context)
{
    /// Filter out hidden columns in the `create table statement`
    ColumnsDescription columns_without_hidden;
    columns_without_hidden.ordinary = columns.ordinary;
    for (const auto & col : columns.materialized)
        if (!hidden_columns.has(col.name))
            columns_without_hidden.materialized.emplace_back(col);
    columns_without_hidden.aliases = columns.aliases;
    columns_without_hidden.defaults = columns.defaults;

    /// If TableInfo from TiDB is empty, for example, create DM table for test,
    /// we refine TableInfo from store's table column, so that we can restore column id next time
    TiDB::TableInfo table_info_from_store;
    if (!table_info_from_tidb)
    {
        table_info_from_store.schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;
        table_info_from_store.name = table_name;
        for (const auto & column_define : store_table_columns)
        {
            if (hidden_columns.has(column_define.name))
                continue;
            TiDB::ColumnInfo column_info = reverseGetColumnInfo(
                NameAndTypePair{column_define.name, column_define.type}, column_define.id, column_define.default_value);
            table_info_from_store.columns.emplace_back(std::move(column_info));
        }
    }

    // We need to update the JSON field in table ast
    // engine = DeltaMerge((CounterID, EventDate), '{JSON format table info}')
    IDatabase::ASTModifier storage_modifier = [&](IAST & ast) {
        std::shared_ptr<ASTLiteral> literal;
        if (table_info_from_tidb)
            literal = std::make_shared<ASTLiteral>(Field(table_info_from_tidb->get().serialize()));
        else
            literal = std::make_shared<ASTLiteral>(Field(table_info_from_store.serialize()));
        auto & storage_ast = typeid_cast<ASTStorage &>(ast);
        auto & args = typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments);
        if (args.children.size() == 1)
            args.children.emplace_back(literal);
        else if (args.children.size() == 2)
            args.children.back() = literal;
        else
            throw Exception(
                "Wrong arguments num:" + DB::toString(args.children.size()) + " in table: " + table_name + " with engine=DeltaMerge",
                ErrorCodes::BAD_ARGUMENTS);
    };

    context.getDatabase(database_name)->alterTable(context, table_name, columns_without_hidden, storage_modifier);
}


BlockInputStreamPtr StorageDeltaMerge::status()
{
    Block block;

    block.insert({std::make_shared<DataTypeString>(), "Name"});
    block.insert({std::make_shared<DataTypeUInt64>(), "Int"});
    block.insert({std::make_shared<DataTypeFloat64>(), "Float"});

    auto columns = block.mutateColumns();
    auto & name_col = columns[0];
    auto & int_col = columns[1];
    auto & float_col = columns[2];

    DeltaMergeStoreStat stat = store->getStat();

    /*

    UInt64 segment_count             = 0;
    UInt64 segment_count_with_delta  = 0;
    UInt64 segment_count_with_stable = 0;

    UInt64 total_rows          = 0;
    UInt64 total_bytes         = 0;
    UInt64 total_delete_ranges = 0;

    Float64 delta_placed_rate = 0;

    Float64 avg_segment_rows  = 0;
    Float64 avg_segment_bytes = 0;

    UInt64  delta_count             = 0;
    UInt64  total_delta_rows        = 0;
    UInt64  total_delta_bytes       = 0;
    Float64 avg_delta_rows          = 0;
    Float64 avg_delta_bytes         = 0;
    Float64 avg_delta_delete_ranges = 0;

    UInt64  stable_count       = 0;
    UInt64  total_stable_rows  = 0;
    UInt64  total_stable_bytes = 0;
    Float64 avg_stable_rows    = 0;
    Float64 avg_stable_bytes   = 0;

    UInt64  total_chunk_count_in_delta = 0;
    Float64 avg_chunk_count_in_delta   = 0;
    Float64 avg_chunk_rows_in_delta    = 0;
    Float64 avg_chunk_bytes_in_delta   = 0;

    UInt64  total_chunk_count_in_stable = 0;
    Float64 avg_chunk_count_in_stable   = 0;
    Float64 avg_chunk_rows_in_stable    = 0;
    Float64 avg_chunk_bytes_in_stable   = 0;
 */

#define INSERT_INT(NAME)             \
    name_col->insert(String(#NAME)); \
    int_col->insert(stat.NAME);      \
    float_col->insert((Float64)0);

#define INSERT_FLOAT(NAME)           \
    name_col->insert(String(#NAME)); \
    int_col->insert((UInt64)0);      \
    float_col->insert(stat.NAME);

    INSERT_INT(segment_count)
    INSERT_INT(segment_count_with_delta)
    INSERT_INT(segment_count_with_stable)

    INSERT_INT(total_rows)
    INSERT_INT(total_bytes)
    INSERT_INT(total_delete_ranges)

    INSERT_FLOAT(delta_placed_rate)

    INSERT_FLOAT(avg_segment_rows)
    INSERT_FLOAT(avg_segment_bytes)

    INSERT_INT(delta_count)
    INSERT_INT(total_delta_rows)
    INSERT_INT(total_delta_bytes)
    INSERT_FLOAT(avg_delta_rows)
    INSERT_FLOAT(avg_delta_bytes)
    INSERT_FLOAT(avg_delta_delete_ranges)

    INSERT_INT(stable_count)
    INSERT_INT(total_stable_rows)
    INSERT_INT(total_stable_bytes)
    INSERT_FLOAT(avg_stable_rows)
    INSERT_FLOAT(avg_stable_bytes)

    INSERT_INT(total_chunk_count_in_delta)
    INSERT_FLOAT(avg_chunk_count_in_delta)
    INSERT_FLOAT(avg_chunk_rows_in_delta)
    INSERT_FLOAT(avg_chunk_bytes_in_delta)

    INSERT_INT(total_chunk_count_in_stable)
    INSERT_FLOAT(avg_chunk_count_in_stable)
    INSERT_FLOAT(avg_chunk_rows_in_stable)
    INSERT_FLOAT(avg_chunk_bytes_in_stable)

#undef INSERT_INT
#undef INSERT_FLOAT

    return std::make_shared<OneBlockInputStream>(block);
}

void StorageDeltaMerge::startup()
{
    TMTContext & tmt = global_context.getTMTContext();
    tmt.getStorages().put(std::static_pointer_cast<StorageDeltaMerge>(shared_from_this()));
}

void StorageDeltaMerge::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;

    // remove this table from TMTContext
    TMTContext & tmt_context = global_context.getTMTContext();
    tmt_context.getStorages().remove(tidb_table_info.id);
    tmt_context.getRegionTable().removeTable(tidb_table_info.id);
}

StorageDeltaMerge::~StorageDeltaMerge() { shutdown(); }

DataTypePtr StorageDeltaMerge::getPKTypeImpl() const { return store->getPKDataType(); }

SortDescription StorageDeltaMerge::getPrimarySortDescription() const { return store->getPrimarySortDescription(); }

} // namespace DB

#include <Common/TiFlashMetrics.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/isSupportedDataTypeCast.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <common/ThreadPool.h>
#include <common/config_common.h>

#include <random>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int DIRECTORY_ALREADY_EXISTS;
} // namespace ErrorCodes

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
      max_column_id_used(0),
      global_context(global_context_.getGlobalContext()),
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
    store = std::make_shared<DeltaMergeStore>(global_context, path, db_name_, table_name_, std::move(table_column_defines),
        std::move(handle_column_define), DeltaMergeStore::Settings());
}

void StorageDeltaMerge::drop()
{
    shutdown();
    store->drop();
}

Block StorageDeltaMerge::buildInsertBlock(bool is_import, bool is_delete, const Block & old_block)
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
        UInt8 tag = is_delete ? 1 : 0;
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = tag;
        }

        addColumnToBlock(block, TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE, std::move(column));
    }

    // Set the real column id.
    auto header = store->getHeader();
    for (auto & col : block)
    {
        if (col.name == EXTRA_HANDLE_COLUMN_NAME)
            col.column_id = EXTRA_HANDLE_COLUMN_ID;
        else if (col.name == VERSION_COLUMN_NAME)
            col.column_id = VERSION_COLUMN_ID;
        else if (col.name == TAG_COLUMN_NAME)
            col.column_id = TAG_COLUMN_ID;
        else
            col.column_id = header->getByName(col.name).column_id;
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

    Block getHeader() const override { return *header; }

    void write(const Block & block) override
    try
    {
        if (db_settings.dt_insert_max_rows == 0)
        {
            store->write(db_context, db_settings, decorator(block));
        }
        else
        {
            Block new_block = decorator(block);
            auto rows = new_block.rows();
            size_t step = db_settings.dt_insert_max_rows;

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
    catch (DB::Exception & e)
    {
        e.addMessage("(while writing to table `" + store->getDatabaseName() + "`.`" + store->getTableName() + "`)");
        throw;
    }

private:
    DeltaMergeStorePtr store;
    BlockPtr header;
    BlockDecorator decorator;
    const Context & db_context;
    const Settings & db_settings;
};

BlockOutputStreamPtr StorageDeltaMerge::write(const ASTPtr & query, const Settings & settings)
{
    auto & insert_query = typeid_cast<const ASTInsertQuery &>(*query);
    auto decorator = [&](const Block & block) { //
        return this->buildInsertBlock(insert_query.is_import, insert_query.is_delete, block);
    };
    return std::make_shared<DMBlockOutputStream>(store, decorator, global_context, settings);
}

void StorageDeltaMerge::write(Block && block, const Settings & settings)
{
    {
        // TODO: remove this code if the column ids in the block are already settled.
        auto header = store->getHeader();
        for (auto & col : block)
        {
            if (col.name == EXTRA_HANDLE_COLUMN_NAME)
                col.column_id = EXTRA_HANDLE_COLUMN_ID;
            else if (col.name == VERSION_COLUMN_NAME)
                col.column_id = VERSION_COLUMN_ID;
            else if (col.name == TAG_COLUMN_NAME)
                col.column_id = TAG_COLUMN_ID;
            else
                col.column_id = header->getByName(col.name).column_id;
        }
    }
    store->write(global_context, settings, block);
}

namespace
{

void throwRetryRegion(const MvccQueryInfo::RegionsQueryInfo & regions_info, RegionException::RegionReadStatus status)
{
    std::vector<RegionID> region_ids;
    region_ids.reserve(regions_info.size());
    for (const auto & info : regions_info)
        region_ids.push_back(info.region_id);
    throw RegionException(std::move(region_ids), status);
}

/// Check if region is invalid.
RegionException::RegionReadStatus isValidRegion(const RegionQueryInfo & region_to_query, const RegionPtr & region_in_mem)
{
    if (region_in_mem->isPendingRemove())
        return RegionException::RegionReadStatus::PENDING_REMOVE;

    const auto & [version, conf_ver, key_range] = region_in_mem->dumpVersionRange();
    (void)key_range;
    if (version != region_to_query.version || conf_ver != region_to_query.conf_version)
        return RegionException::RegionReadStatus::VERSION_ERROR;

    return RegionException::RegionReadStatus::OK;
}

RegionMap doLearnerRead(const TiDB::TableID table_id,           //
    const MvccQueryInfo::RegionsQueryInfo & regions_query_info, //
    const bool resolve_locks, const Timestamp start_ts,
    size_t concurrent_num, //
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
            regions_info.emplace_back(
                RegionQueryInfo{id, region->version(), region->confVer(), region->getHandleRangeByTable(table_id), {}});
        }
    }

    // adjust concurrency by num of regions
    concurrent_num = std::max(1, std::min(concurrent_num, regions_info.size()));

    KVStorePtr & kvstore = tmt.getKVStore();
    Context & context = tmt.getContext();
    RegionMap kvstore_region;
    // check region is not null and store region map.
    for (const auto & info : regions_info)
    {
        auto region = kvstore->getRegion(info.region_id);
        if (region == nullptr)
        {
            LOG_WARNING(log, "[region " << info.region_id << "] is not found in KVStore, try again");
            throwRetryRegion(regions_info, RegionException::RegionReadStatus::NOT_FOUND);
        }
        kvstore_region.emplace(info.region_id, std::move(region));
    }
    // make sure regions are not duplicated.
    if (unlikely(kvstore_region.size() != regions_info.size()))
        throw Exception("Duplicate region id", ErrorCodes::LOGICAL_ERROR);

    auto metrics = context.getTiFlashMetrics();
    const size_t num_regions = regions_info.size();
    const size_t batch_size = num_regions / concurrent_num;
    std::atomic_uint8_t region_status = RegionException::RegionReadStatus::OK;
    const auto batch_wait_index = [&, resolve_locks, start_ts](const size_t region_begin_idx) -> void {
        const size_t region_end_idx = std::min(region_begin_idx + batch_size, num_regions);
        for (size_t region_idx = region_begin_idx; region_idx < region_end_idx; ++region_idx)
        {
            // If any threads meets an error, just return.
            if (region_status != RegionException::RegionReadStatus::OK)
                return;

            RegionQueryInfo & region_to_query = regions_info[region_idx];
            const RegionID region_id = region_to_query.region_id;
            auto region = kvstore_region[region_id];

            auto status = isValidRegion(region_to_query, region);
            if (status != RegionException::RegionReadStatus::OK)
            {
                region_status = status;
                LOG_WARNING(log,
                    "Check memory cache, region " << region_id << ", version " << region_to_query.version << ", handle range ["
                                                  << region_to_query.range_in_table.first.toString() << ", "
                                                  << region_to_query.range_in_table.second.toString() << ") , status "
                                                  << RegionException::RegionReadStatusString(status));
                return;
            }

            GET_METRIC(metrics, tiflash_raft_read_index_count).Increment();
            Stopwatch read_index_watch;

            /// Blocking learner read. Note that learner read must be performed ahead of data read,
            /// otherwise the desired index will be blocked by the lock of data read.
            auto read_index_result = region->learnerRead();
            GET_METRIC(metrics, tiflash_raft_read_index_duration_seconds).Observe(read_index_watch.elapsedSeconds());
            if (read_index_result.region_unavailable)
            {
                // client-c detect region removed. Set region_status and continue.
                region_status = RegionException::RegionReadStatus::NOT_FOUND;
                continue;
            }
            else if (read_index_result.region_epoch_not_match)
            {
                region_status = RegionException::RegionReadStatus::VERSION_ERROR;
                continue;
            }
            else
            {
                Stopwatch wait_index_watch;
                if (region->waitIndex(read_index_result.read_index, tmt.getTerminated()))
                {
                    region_status = RegionException::RegionReadStatus::NOT_FOUND;
                    continue;
                }
                GET_METRIC(metrics, tiflash_raft_wait_index_duration_seconds).Observe(wait_index_watch.elapsedSeconds());
            }
            if (resolve_locks)
            {
                status = RegionTable::resolveLocksAndWriteRegion( //
                    tmt,                                          //
                    table_id,                                     //
                    region,                                       //
                    start_ts,                                     //
                    region_to_query.version,                      //
                    region_to_query.conf_version,                 //
                    region_to_query.range_in_table, log);

                if (status != RegionException::RegionReadStatus::OK)
                {
                    LOG_WARNING(log,
                        "Check memory cache, region " << region_id << ", version " << region_to_query.version << ", handle range ["
                                                      << region_to_query.range_in_table.first.toString() << ", "
                                                      << region_to_query.range_in_table.second.toString() << ") , status "
                                                      << RegionException::RegionReadStatusString(status));
                    region_status = status;
                }
            }
        }
    };
    auto start_time = Clock::now();
    if (concurrent_num <= 1)
    {
        batch_wait_index(0);
    }
    else
    {
        ::ThreadPool pool(concurrent_num);
        for (size_t region_begin_idx = 0; region_begin_idx < num_regions; region_begin_idx += batch_size)
        {
            pool.schedule([&batch_wait_index, region_begin_idx] { batch_wait_index(region_begin_idx); });
        }
        pool.wait();
    }

    // Check if any region is invalid, TiDB / TiSpark should refresh region cache and retry.
    if (region_status != RegionException::RegionReadStatus::OK)
        throwRetryRegion(regions_info, static_cast<RegionException::RegionReadStatus>(region_status.load()));

    auto end_time = Clock::now();
    LOG_DEBUG(log,
        "[Learner Read] wait index cost " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms");

    /// If background flush is disabled, we don't need to do both flushing KVStore or waiting for background tasks.
    if (!tmt.isBgFlushDisabled())
    {
        // After raft index is satisfied, we flush region to StorageDeltaMerge so that we can read all data
        start_time = Clock::now();
        std::set<RegionID> regions_flushing_in_bg_threads;
        auto & region_table = tmt.getRegionTable();
        for (auto && [region_id, region] : kvstore_region)
        {
            if (!region->dataSize())
                continue;
            auto to_remove_data = region_table.tryFlushRegion(region, false);
            // If region is flushing by other bg threads, we should mark those regions to wait.
            if (to_remove_data.empty())
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

    return kvstore_region;
}

} // namespace

std::unordered_set<UInt64> parseSegmentSet(const ASTPtr & ast)
{
    if (!ast)
        return {};
    const auto & partition_ast = typeid_cast<const ASTPartition &>(*ast);

    if (!partition_ast.value)
        return {parse<UInt64>(partition_ast.id)};

    auto parse_segment_id = [](const ASTLiteral * literal) -> std::pair<bool, UInt64> {
        if (!literal)
            return {false, 0};
        switch (literal->value.getType())
        {
            case Field::Types::String:
                return {true, parse<UInt64>(literal->value.get<String>())};
            case Field::Types::UInt64:
                return {true, literal->value.get<UInt64>()};
            default:
                return {false, 0};
        }
    };

    {
        const auto * partition_lit = typeid_cast<const ASTLiteral *>(partition_ast.value.get());
        auto [suc, id] = parse_segment_id(partition_lit);
        if (suc)
            return {id};
    }

    const auto * partition_function = typeid_cast<const ASTFunction *>(partition_ast.value.get());
    if (partition_function && partition_function->name == "tuple")
    {
        std::unordered_set<UInt64> ids;
        bool ok = true;
        for (const auto & item : partition_function->arguments->children)
        {
            const auto * partition_lit = typeid_cast<const ASTLiteral *>(item.get());
            auto [suc, id] = parse_segment_id(partition_lit);
            if (suc)
            {
                ids.emplace(id);
            }
            else
            {
                ok = false;
                break;
            }
        }
        if (ok)
            return ids;
    }

    throw Exception("Unable to parse segment IDs in literal form: `" + partition_ast.fields_str.toString() + "`");
}

BlockInputStreams StorageDeltaMerge::read( //
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    // Note that `columns_to_read` should keep the same sequence as ColumnRef
    // in `Coprocessor.TableScan.columns`, or rough set filter could be
    // failed to parsed.
    ColumnDefines columns_to_read;
    auto header = store->getHeader();
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
            auto & column = header->getByName(n);
            col_define.name = column.name;
            col_define.id = column.column_id;
            col_define.type = column.type;
            col_define.default_value = column.default_value;
        }
        columns_to_read.push_back(col_define);
    }


    const ASTSelectQuery & select_query = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    if (select_query.raw_for_mutable)
        return store->readRaw(
            context, context.getSettingsRef(), columns_to_read, num_streams, parseSegmentSet(select_query.segment_expression_list));
    else
    {
        if (unlikely(!query_info.mvcc_query_info))
            throw Exception("mvcc query info is null", ErrorCodes::LOGICAL_ERROR);

        TMTContext & tmt = context.getTMTContext();
        if (unlikely(!tmt.isInitialized()))
            throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

        const auto & mvcc_query_info = *query_info.mvcc_query_info;

        LOG_DEBUG(log, "Read with tso: " << mvcc_query_info.read_tso);

        const auto check_read_tso = [&tmt, &context, this](UInt64 read_tso) {
            // Read with specify tso, check if tso is smaller than TiDB GcSafePoint
            auto pd_client = tmt.getPDClient();
            if (likely(!pd_client->isMock()))
            {
                auto safe_point = PDClientHelper::getGCSafePointWithRetry(pd_client,
                    /* ignore_cache= */ false,
                    global_context.getSettingsRef().safe_point_update_interval_seconds);
                if (read_tso < safe_point)
                    throw Exception("query id: " + context.getCurrentQueryId() + ", read tso: " + toString(read_tso)
                            + " is smaller than tidb gc safe point: " + toString(safe_point),
                        ErrorCodes::LOGICAL_ERROR);
            }
        };
        check_read_tso(mvcc_query_info.read_tso);

        /// If request comes from TiDB/TiSpark, mvcc_query_info.concurrent is 0,
        /// and `concurrent_num` should be 1. Concurrency handled by TiDB/TiSpark.
        /// Else a request comes from CH-client, we set `concurrent_num` by num_streams.
        size_t concurrent_num = std::max<size_t>(num_streams * mvcc_query_info.concurrent, 1);

        // With `no_kvstore` is true, we do not do learner read
        RegionMap regions_in_learner_read;
        if (likely(!select_query.no_kvstore))
        {
            /// Learner read.
            regions_in_learner_read = doLearnerRead(tidb_table_info.id, mvcc_query_info.regions_query_info, mvcc_query_info.resolve_locks,
                mvcc_query_info.read_tso, concurrent_num, tmt, log);

            if (likely(!mvcc_query_info.regions_query_info.empty()))
            {
                /// For learner read from TiDB/TiSpark, we set num_streams by `concurrent_num`
                num_streams = concurrent_num;
            } // else learner read from ch-client, keep num_streams
        }

        HandleRanges ranges = getQueryRanges(mvcc_query_info.regions_query_info);

        if (log->trace())
        {
            {
                std::stringstream ss;
                for (const auto & region : mvcc_query_info.regions_query_info)
                {
                    if (!region.required_handle_ranges.empty())
                    {
                        for (const auto & range : region.required_handle_ranges)
                            ss << region.region_id << "[" << range.first.toString() << "," << range.second.toString() << "),";
                    }
                    else
                    {
                        /// only used for test cases
                        const auto & range = region.range_in_table;
                        ss << region.region_id << "[" << range.first.toString() << "," << range.second.toString() << "),";
                    }
                }
                std::stringstream ss_merged_range;
                for (const auto & range : ranges)
                    ss_merged_range << range.toString() << ",";
                LOG_TRACE(log, "reading ranges: orig, " << ss.str() << " merged, " << ss_merged_range.str());
            }
        }

        /// Get Rough set filter from query
        DM::RSOperatorPtr rs_operator = DM::EMPTY_FILTER;
        const bool enable_rs_filter = context.getSettingsRef().dt_enable_rough_set_filter;
        if (enable_rs_filter)
        {
            if (likely(query_info.dag_query))
            {
                /// Query from TiDB / TiSpark
                auto create_attr_by_column_id = [this](ColumnID column_id) -> Attr {
                    const ColumnDefines & defines = this->store->getTableColumns();
                    auto iter = std::find_if(
                        defines.begin(), defines.end(), [column_id](const ColumnDefine & d) -> bool { return d.id == column_id; });
                    if (iter != defines.end())
                        return Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
                    else
                        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
                        return Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
                };
                rs_operator = FilterParser::parseDAGQuery(*query_info.dag_query, columns_to_read, std::move(create_attr_by_column_id), log);
            }
            else
            {
                // Query from ch client
                auto create_attr_by_column_id = [this](const String & col_name) -> Attr {
                    const ColumnDefines & defines = this->store->getTableColumns();
                    auto iter = std::find_if(
                        defines.begin(), defines.end(), [&col_name](const ColumnDefine & d) -> bool { return d.name == col_name; });
                    if (iter != defines.end())
                        return Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
                    else
                        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
                        return Attr{.col_name = col_name, .col_id = 0, .type = DataTypePtr{}};
                };
                rs_operator = FilterParser::parseSelectQuery(select_query, std::move(create_attr_by_column_id), log);
            }
            if (likely(rs_operator != DM::EMPTY_FILTER))
                LOG_DEBUG(log, "Rough set filter: " << rs_operator->toString());
        }
        else
            LOG_DEBUG(log, "Rough set filter is disabled.");


        auto streams = store->read(context, context.getSettingsRef(), columns_to_read, ranges, num_streams,
            /*max_version=*/mvcc_query_info.read_tso, rs_operator, max_block_size, parseSegmentSet(select_query.segment_expression_list));

        {
            /// Ensure read_tso and regions' info after read.

            check_read_tso(mvcc_query_info.read_tso);

            for (const auto & region_query_info : mvcc_query_info.regions_query_info)
            {
                RegionException::RegionReadStatus status = RegionException::RegionReadStatus::OK;
                auto region = tmt.getKVStore()->getRegion(region_query_info.region_id);
                if (region != regions_in_learner_read[region_query_info.region_id])
                    status = RegionException::RegionReadStatus::NOT_FOUND;
                else if (region->version() != region_query_info.version)
                {
                    // ABA problem may cause because one region is removed and inserted back.
                    // if the version of region is changed, the `streams` may has less data because of compaction.
                    status = RegionException::RegionReadStatus::VERSION_ERROR;
                }

                if (status != RegionException::RegionReadStatus::OK)
                {
                    LOG_WARNING(log,
                        "Check after read from DeltaMergeStore, region "
                            << region_query_info.region_id << ", version " << region_query_info.version //
                            << ", handle range [" << region_query_info.range_in_table.first.toString() << ", "
                            << region_query_info.range_in_table.second.toString() << "), status "
                            << RegionException::RegionReadStatusString(status));
                    // throw region exception and let TiDB retry
                    throwRetryRegion(mvcc_query_info.regions_query_info, status);
                }
            }
        }

        return streams;
    }
}

void StorageDeltaMerge::checkStatus(const Context & context) { store->check(context); }

void StorageDeltaMerge::flushCache(const Context & context, const DM::HandleRange & range_to_flush)
{
    store->flushCache(context, range_to_flush);
}

void StorageDeltaMerge::mergeDelta(const Context & context) { store->mergeDeltaAll(context); }

void StorageDeltaMerge::deleteRange(const DM::HandleRange & range_to_delete, const Settings & settings)
{
    auto metrics = global_context.getTiFlashMetrics();
    GET_METRIC(metrics, tiflash_storage_command_count, type_delete_range).Increment();
    return store->deleteRange(global_context, settings, range_to_delete);
}

size_t getRows(DM::DeltaMergeStorePtr & store, const Context & context, const DM::HandleRange & range)
{
    size_t rows = 0;

    ColumnDefines to_read{getExtraHandleColumnDefine()};
    auto stream = store->read(context, context.getSettingsRef(), to_read, {range}, 1, MAX_UINT64, EMPTY_FILTER)[0];
    stream->readPrefix();
    Block block;
    while ((block = stream->read()))
        rows += block.rows();
    stream->readSuffix();

    return rows;
}

DM::HandleRange getRange(DM::DeltaMergeStorePtr & store, const Context & context, size_t total_rows, size_t delete_rows)
{
    auto start_index = rand() % (total_rows - delete_rows + 1);

    DM::HandleRange range = DM::HandleRange::newAll();
    {
        ColumnDefines to_read{getExtraHandleColumnDefine()};
        auto stream = store->read(context, context.getSettingsRef(), to_read, {DM::HandleRange::newAll()}, 1, MAX_UINT64, EMPTY_FILTER)[0];
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
                if (index == start_index + delete_rows)
                    range.end = data[i];
                ++index;
            }
        }
        stream->readSuffix();
    }

    return range;
}

void StorageDeltaMerge::deleteRows(const Context & context, size_t delete_rows)
{
    size_t total_rows = getRows(store, context, DM::HandleRange::newAll());
    delete_rows = std::min(total_rows, delete_rows);
    auto delete_range = getRange(store, context, total_rows, delete_rows);
    size_t actual_delete_rows = getRows(store, context, delete_range);
    if (actual_delete_rows != delete_rows)
        LOG_ERROR(log, "Expected delete rows: " << delete_rows << ", got: " << actual_delete_rows);

    store->deleteRange(context, context.getSettingsRef(), delete_range);

    size_t after_delete_rows = getRows(store, context, DM::HandleRange::newAll());
    if (after_delete_rows != total_rows - delete_rows)
        LOG_ERROR(log, "Rows after delete range not match, expected: " << (total_rows - delete_rows) << ", got: " << after_delete_rows);
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

String StorageDeltaMerge::getName() const { return MutableSupport::delta_tree_storage_name; }

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

    // remove background tasks
    store->shutdown();
    // rename directories for multi disks
    store->rename(new_path, new_database_name, new_table_name);

    store = {}; // reset store object

    // generate a new store
    store = std::make_shared<DeltaMergeStore>(global_context, //
        new_path, new_database_name, new_table_name,          //
        std::move(table_column_defines), std::move(handle_column_define), settings);

    path = new_path;
}

String StorageDeltaMerge::getTableName() const { return store->getTableName(); }

String StorageDeltaMerge::getDatabaseName() const { return store->getDatabaseName(); }

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
            TiDB::ColumnInfo column_info = reverseGetColumnInfo( //
                NameAndTypePair{column_define.name, column_define.type}, column_define.id, column_define.default_value,
                /* for_test= */ true);
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
            throw Exception("Wrong arguments num:" + DB::toString(args.children.size()) + " in table: " + table_name
                    + " with engine=" + MutableSupport::delta_tree_storage_name,
                ErrorCodes::BAD_ARGUMENTS);
    };

    context.getDatabase(database_name)->alterTable(context, table_name, columns_without_hidden, storage_modifier);
}


BlockInputStreamPtr StorageDeltaMerge::status()
{
    Block block;

    block.insert({std::make_shared<DataTypeString>(), "Name"});
    block.insert({std::make_shared<DataTypeString>(), "Value"});

    auto columns = block.mutateColumns();
    auto & name_col = columns[0];
    auto & value_col = columns[1];

    DeltaMergeStoreStat stat = store->getStat();

#define INSERT_INT(NAME)             \
    name_col->insert(String(#NAME)); \
    value_col->insert(DB::toString(stat.NAME));

#define INSERT_SIZE(NAME)            \
    name_col->insert(String(#NAME)); \
    value_col->insert(formatReadableSizeWithBinarySuffix(stat.NAME, 2));

#define INSERT_RATE(NAME)            \
    name_col->insert(String(#NAME)); \
    value_col->insert(DB::toString(stat.NAME * 100, 2) + "%");

#define INSERT_FLOAT(NAME)           \
    name_col->insert(String(#NAME)); \
    value_col->insert(DB::toString(stat.NAME, 2));

    INSERT_INT(segment_count)
    INSERT_INT(total_rows)
    INSERT_SIZE(total_size)
    INSERT_INT(total_delete_ranges)

    INSERT_SIZE(total_delta_size)
    INSERT_SIZE(total_stable_size)

    INSERT_RATE(delta_rate_rows)
    INSERT_RATE(delta_rate_segments)

    INSERT_RATE(delta_placed_rate)
    INSERT_SIZE(delta_cache_size)
    INSERT_RATE(delta_cache_rate)
    INSERT_RATE(delta_cache_wasted_rate)

    INSERT_FLOAT(avg_segment_rows)
    INSERT_SIZE(avg_segment_size)

    INSERT_INT(delta_count)
    INSERT_INT(total_delta_rows)
    INSERT_FLOAT(avg_delta_rows)
    INSERT_FLOAT(avg_delta_delete_ranges)

    INSERT_INT(stable_count)
    INSERT_INT(total_stable_rows)
    INSERT_FLOAT(avg_stable_rows)

    INSERT_INT(total_pack_count_in_delta)
    INSERT_FLOAT(avg_pack_count_in_delta)
    INSERT_FLOAT(avg_pack_rows_in_delta)
    INSERT_SIZE(avg_pack_size_in_delta)

    INSERT_INT(total_pack_count_in_stable)
    INSERT_FLOAT(avg_pack_count_in_stable)
    INSERT_FLOAT(avg_pack_rows_in_stable)
    INSERT_SIZE(avg_pack_size_in_stable)

    INSERT_INT(storage_stable_num_snapshots);
    INSERT_INT(storage_stable_num_pages);
    INSERT_INT(storage_stable_num_normal_pages)
    INSERT_INT(storage_stable_max_page_id);

    INSERT_INT(storage_delta_num_snapshots);
    INSERT_INT(storage_delta_num_pages);
    INSERT_INT(storage_delta_num_normal_pages)
    INSERT_INT(storage_delta_max_page_id);

    INSERT_INT(storage_meta_num_snapshots);
    INSERT_INT(storage_meta_num_pages);
    INSERT_INT(storage_meta_num_normal_pages)
    INSERT_INT(storage_meta_max_page_id);

    INSERT_INT(background_tasks_length);

#undef INSERT_INT
#undef INSERT_RATE
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
    bool v = false;
    if (!shutdown_called.compare_exchange_strong(v, true))
        return;

    store->shutdown();
}

void StorageDeltaMerge::removeFromTMTContext()
{
    // remove this table from TMTContext
    TMTContext & tmt_context = global_context.getTMTContext();
    tmt_context.getStorages().remove(tidb_table_info.id);
    tmt_context.getRegionTable().removeTable(tidb_table_info.id);
}

StorageDeltaMerge::~StorageDeltaMerge() { shutdown(); }

DataTypePtr StorageDeltaMerge::getPKTypeImpl() const { return store->getPKDataType(); }

SortDescription StorageDeltaMerge::getPrimarySortDescription() const { return store->getPrimarySortDescription(); }

} // namespace DB

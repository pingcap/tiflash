#include <Flash/Coprocessor/InterpreterDAG.h>

#include <Core/TMTPKType.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/Aggregator.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int TOO_MANY_COLUMNS;
extern const int SCHEMA_VERSION_ERROR;
extern const int UNKNOWN_EXCEPTION;
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

InterpreterDAG::InterpreterDAG(Context & context_, const DAGQuerySource & dag_)
    : context(context_),
      dag(dag_),
      keep_session_timezone_info(dag.getEncodeType() == tipb::EncodeType::TypeArrow),
      log(&Logger::get("InterpreterDAG"))
{}

template <typename HandleType>
bool isAllValueCoveredByRanges(std::vector<HandleRange<HandleType>> & ranges, const std::vector<HandleRange<HandleType>> & region_ranges)
{
    if (ranges.empty())
        return false;
    std::sort(ranges.begin(), ranges.end(),
        [](const HandleRange<HandleType> & a, const HandleRange<HandleType> & b) { return a.first < b.first; });

    std::vector<HandleRange<HandleType>> merged_ranges;
    HandleRange<HandleType> merged_range;
    merged_range.first = ranges[0].first;
    merged_range.second = ranges[0].second;

    for (size_t i = 1; i < ranges.size(); i++)
    {
        if (merged_range.second >= ranges[i].first)
            merged_range.second = merged_range.second >= ranges[i].second ? merged_range.second : ranges[i].second;
        else
        {
            merged_ranges.emplace_back(std::make_pair(merged_range.first, merged_range.second));
            merged_range.first = ranges[i].first;
            merged_range.second = ranges[i].second;
        }
    }
    merged_ranges.emplace_back(std::make_pair(merged_range.first, merged_range.second));

    for (const auto & region_range : region_ranges)
    {
        bool covered = false;
        for (const auto & range : merged_ranges)
        {
            if (region_range.first >= range.first && region_range.second <= range.second)
            {
                covered = true;
                break;
            }
        }
        if (!covered && region_range.second > region_range.first)
            return false;
    }
    return true;
}

bool checkKeyRanges(const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges, TableID table_id, bool pk_is_uint64,
    const ImutRegionRangePtr & region_key_range)
{
    if (key_ranges.empty())
        return true;

    std::vector<HandleRange<Int64>> handle_ranges;
    for (auto & range : key_ranges)
    {
        TiKVRange::Handle start = TiKVRange::getRangeHandle<true>(range.first, table_id);
        TiKVRange::Handle end = TiKVRange::getRangeHandle<false>(range.second, table_id);
        handle_ranges.emplace_back(std::make_pair(start, end));
    }

    std::vector<HandleRange<Int64>> region_handle_ranges;
    auto & raw_keys = region_key_range->rawKeys();
    TiKVRange::Handle region_start = TiKVRange::getRangeHandle<true>(raw_keys.first, table_id);
    TiKVRange::Handle region_end = TiKVRange::getRangeHandle<false>(raw_keys.second, table_id);
    region_handle_ranges.emplace_back(std::make_pair(region_start, region_end));

    if (pk_is_uint64)
    {
        std::vector<HandleRange<UInt64>> update_handle_ranges;
        for (auto & range : handle_ranges)
        {
            const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(range);

            for (int i = 0; i < n; i++)
            {
                update_handle_ranges.emplace_back(new_range[i]);
            }
        }
        std::vector<HandleRange<UInt64>> update_region_handle_ranges;
        for (auto & range : region_handle_ranges)
        {
            const auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle(range);

            for (int i = 0; i < n; i++)
            {
                update_region_handle_ranges.emplace_back(new_range[i]);
            }
        }
        return isAllValueCoveredByRanges<UInt64>(update_handle_ranges, update_region_handle_ranges);
    }
    else
        return isAllValueCoveredByRanges<Int64>(handle_ranges, region_handle_ranges);
}

RegionException::RegionReadStatus InterpreterDAG::getRegionReadStatus(RegionPtr current_region)
{
    if (!current_region)
        return RegionException::NOT_FOUND;
    if (current_region->version() != dag.getRegionVersion() || current_region->confVer() != dag.getRegionConfVersion())
        return RegionException::VERSION_ERROR;
    if (current_region->isPendingRemove())
        return RegionException::PENDING_REMOVE;
    return RegionException::OK;
}

// the flow is the same as executeFetchcolumns
void InterpreterDAG::executeTS(const tipb::TableScan & ts, Pipeline & pipeline)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw Exception("Table id not specified in table scan executor", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    TableID table_id = ts.table_id();
    // TODO: Get schema version from DAG request.
    if (context.getSettingsRef().schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION)
    {
        storage = context.getTMTContext().getStorages().get(table_id);
        if (storage == nullptr)
        {
            throw Exception("Table " + std::to_string(table_id) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        }
        table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
    }
    else
    {
        getAndLockStorageWithSchemaVersion(table_id, DEFAULT_UNSPECIFIED_SCHEMA_VERSION);
    }

    Names required_columns;
    std::vector<NameAndTypePair> source_columns;
    std::vector<bool> is_ts_column;
    for (const tipb::ColumnInfo & ci : ts.columns())
    {
        ColumnID cid = ci.column_id();

        if (cid == -1)
        {
            // Column ID -1 return the handle column
            if (auto pk_handle_col = storage->getTableInfo().getPKHandleColumn())
            {
                required_columns.push_back(pk_handle_col->get().name);
                auto pair = storage->getColumns().getPhysical(pk_handle_col->get().name);
                source_columns.push_back(pair);
                is_ts_column.push_back(false);
            }
            else
            {
                required_columns.push_back(MutableSupport::tidb_pk_column_name);
                auto pair = storage->getColumns().getPhysical(MutableSupport::tidb_pk_column_name);
                source_columns.push_back(pair);
                is_ts_column.push_back(false);
            }
            continue;
        }

        String name = storage->getTableInfo().getColumnName(cid);
        required_columns.push_back(name);
        auto pair = storage->getColumns().getPhysical(name);
        source_columns.emplace_back(std::move(pair));
        is_ts_column.push_back(ci.tp() == TiDB::TypeTimestamp);
    }

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    if (!dag.hasAggregation())
    {
        // if the dag request does not contain agg, then the final output is
        // based on the output of table scan
        for (auto i : dag.getDAGRequest().output_offsets())
        {
            if (i >= required_columns.size())
            {
                // array index out of bound
                throw Exception("Output offset index is out of bound", ErrorCodes::COP_BAD_DAG_REQUEST);
            }
            // do not have alias
            final_project.emplace_back(required_columns[i], "");
        }
    }
    // todo handle alias column
    const Settings & settings = context.getSettingsRef();

    if (settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
    {
        throw Exception("Limit for number of columns to read exceeded. "
                        "Requested: "
                + toString(required_columns.size()) + ", maximum: " + settings.max_columns_to_read.toString(),
            ErrorCodes::TOO_MANY_COLUMNS);
    }

    size_t max_block_size = settings.max_block_size;
    max_streams = settings.max_threads;
    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
    if (max_streams > 1)
    {
        max_streams *= settings.max_streams_to_max_threads_ratio;
    }

    if (dag.hasSelection())
    {
        for (auto & condition : dag.getSelection().conditions())
        {
            analyzer->makeExplicitSetForIndex(condition, storage);
        }
    }
    //todo support index in
    SelectQueryInfo query_info;
    // set query to avoid unexpected NPE
    query_info.query = dag.getAST();
    query_info.dag_query = std::make_unique<DAGQueryInfo>(dag, analyzer->getPreparedSets(), analyzer->getCurrentInputColumns());
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>();
    query_info.mvcc_query_info->resolve_locks = true;
    query_info.mvcc_query_info->read_tso = settings.read_tso;
    RegionQueryInfo info;
    info.region_id = dag.getRegionID();
    info.version = dag.getRegionVersion();
    info.conf_version = dag.getRegionConfVersion();
    auto current_region = context.getTMTContext().getKVStore()->getRegion(info.region_id);
    auto region_read_status = getRegionReadStatus(current_region);
    if (region_read_status != RegionException::OK)
    {
        std::vector<RegionID> region_ids;
        region_ids.push_back(info.region_id);
        throw RegionException(std::move(region_ids), region_read_status);
    }
    if (!checkKeyRanges(dag.getKeyRanges(), table_id, /* pk_is_uint64= */ storage->getPKType() == IManageableStorage::PKType::UINT64,
            current_region->getRange()))
        throw Exception("Cop request only support full range scan for given region", ErrorCodes::COP_BAD_DAG_REQUEST);
    info.range_in_table = current_region->getHandleRangeByTable(table_id);
    query_info.mvcc_query_info->regions_query_info.push_back(info);
    query_info.mvcc_query_info->concurrent = 0.0;
    pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams);

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(std::make_shared<NullBlockInputStream>(storage->getSampleBlockForColumns(required_columns)));
    }

    pipeline.transform([&](auto & stream) { stream->addTableLock(table_lock); });

    /// Set the limits and quota for reading data, the speed and time of the query.
    {
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
        limits.max_execution_time = settings.max_execution_time;
        limits.timeout_overflow_mode = settings.timeout_overflow_mode;

        /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
              *  because the initiating server has a summary of the execution of the request on all servers.
              *
              * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
              *  additionally on each remote server, because these limits are checked per block of data processed,
              *  and remote servers may process way more blocks of data than are received by initiator.
              */
        limits.min_execution_speed = settings.min_execution_speed;
        limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

        QuotaForIntervals & quota = context.getQuota();

        pipeline.transform([&](auto & stream) {
            if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
            {
                p_stream->setLimits(limits);
                p_stream->setQuota(quota);
            }
        });
    }

    if (addTimeZoneCastAfterTS(is_ts_column, pipeline))
    {
        // for arrow encode, the final select of timestamp column should be column with session timezone
        if (keep_session_timezone_info && !dag.hasAggregation())
        {
            for (auto i : dag.getDAGRequest().output_offsets())
            {
                if (is_ts_column[i])
                {
                    final_project[i].first = analyzer->getCurrentInputColumns()[i].name;
                }
            }
        }
    }
}

// add timezone cast for timestamp type, this is used to support session level timezone
bool InterpreterDAG::addTimeZoneCastAfterTS(std::vector<bool> & is_ts_column, Pipeline & pipeline)
{
    bool hasTSColumn = false;
    for (auto b : is_ts_column)
        hasTSColumn |= b;
    if (!hasTSColumn)
        return false;

    ExpressionActionsChain chain;
    if (analyzer->appendTimeZoneCastsAfterTS(chain, is_ts_column, dag.getDAGRequest()))
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions()); });
        return true;
    }
    else
        return false;
}

InterpreterDAG::AnalysisResult InterpreterDAG::analyzeExpressions()
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    if (dag.hasSelection())
    {
        analyzer->appendWhere(chain, dag.getSelection(), res.filter_column_name);
        res.has_where = true;
        res.before_where = chain.getLastActions();
        chain.addStep();
    }
    // There will be either Agg...
    if (dag.hasAggregation())
    {
        analyzer->appendAggregation(chain, dag.getAggregation(), res.aggregation_keys, res.aggregate_descriptions);
        res.need_aggregate = true;
        res.before_aggregation = chain.getLastActions();

        chain.finalize();
        chain.clear();

        // add cast if type is not match
        analyzer->appendAggSelect(chain, dag.getAggregation(), dag.getDAGRequest(), keep_session_timezone_info);
        //todo use output_offset to reconstruct the final project columns
        for (auto element : analyzer->getCurrentInputColumns())
        {
            final_project.emplace_back(element.name, "");
        }
    }
    // Or TopN, not both.
    if (dag.hasTopN())
    {
        res.has_order_by = true;
        analyzer->appendOrderBy(chain, dag.getTopN(), res.order_column_names);
    }
    // Append final project results if needed.
    analyzer->appendFinalProject(chain, final_project);
    res.before_order_and_select = chain.getLastActions();
    chain.finalize();
    chain.clear();
    //todo need call prependProjectInput??
    return res;
}

void InterpreterDAG::executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expr, String & filter_column)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column); });
}

void InterpreterDAG::executeAggregation(
    Pipeline & pipeline, const ExpressionActionsPtr & expr, Names & key_names, AggregateDescriptions & aggregates)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expr); });

    Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : key_names)
    {
        keys.push_back(header.getPositionByName(name));
    }
    for (auto & descr : aggregates)
    {
        if (descr.arguments.empty())
        {
            for (const auto & name : descr.argument_names)
            {
                descr.arguments.push_back(header.getPositionByName(name));
            }
        }
    }

    const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;

    Aggregator::Params params(header, keys, aggregates, false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
        settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set, context.getTemporaryPath());

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(pipeline.streams, nullptr, params, true, max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                                                                : static_cast<size_t>(settings.max_threads));

        pipeline.streams.resize(1);
    }
    else
    {
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(pipeline.firstStream(), params, true);
    }
    // add cast
}

void InterpreterDAG::executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr); });
    }
}

void InterpreterDAG::getAndLockStorageWithSchemaVersion(TableID table_id, Int64 query_schema_version)
{
    /// Get current schema version in schema syncer for a chance to shortcut.
    auto global_schema_version = context.getTMTContext().getSchemaSyncer()->getCurrentVersion();

    /// Lambda for get storage, then align schema version under the read lock.
    auto get_and_lock_storage = [&](bool schema_synced) -> std::tuple<ManageableStoragePtr, TableStructureReadLockPtr, Int64, bool> {
        /// Get storage in case it's dropped then re-created.
        // If schema synced, call getTable without try, leading to exception on table not existing.
        auto storage_ = context.getTMTContext().getStorages().get(table_id);
        if (!storage_)
        {
            if (schema_synced)
                throw Exception("Table " + std::to_string(table_id) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
            else
                return std::make_tuple(nullptr, nullptr, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, false);
        }

        if (storage_->engineType() != ::TiDB::StorageEngine::TMT && storage_->engineType() != ::TiDB::StorageEngine::DM)
        {
            throw Exception("Specifying schema_version for non-managed storage: " + storage_->getName() + ", table: " + storage_->getTableName()
                    + ",id: " + DB::toString(table_id) + " is not allowed",
                ErrorCodes::LOGICAL_ERROR);
        }

        /// Lock storage.
        auto lock = storage_->lockStructure(false, __PRETTY_FUNCTION__);

        /// Check schema version.
        auto storage_schema_version = storage_->getTableInfo().schema_version;
        // Not allow storage schema version greater than query schema version in any case.
        if (storage_schema_version > query_schema_version)
            throw Exception("Table " + std::to_string(table_id) + " schema version " + std::to_string(storage_schema_version)
                    + " newer than query schema version " + std::to_string(query_schema_version),
                ErrorCodes::SCHEMA_VERSION_ERROR);

        // If schema synced, we must be very recent so we are good as long as storage schema version is no greater than query schema version.
        // If schema not synced, we are good if storage schema version is right on query schema version.
        // Otherwise we are at the risk of out-of-date schema, but we still have a chance to be sure that we are good, if global schema version is greater than query schema version.
        if ((schema_synced && storage_schema_version <= query_schema_version)
            || (!schema_synced && (storage_schema_version == query_schema_version || global_schema_version > query_schema_version)))
            return std::make_tuple(storage_, lock, storage_schema_version, true);

        return std::make_tuple(nullptr, nullptr, storage_schema_version, false);
    };

    /// Try get storage and lock once.
    ManageableStoragePtr storage_;
    TableStructureReadLockPtr lock;
    Int64 storage_schema_version;
    auto log_schema_version = [&](const String & result) {
        LOG_DEBUG(log,
            __PRETTY_FUNCTION__ << " Table " << table_id << " schema " << result << " Schema version [storage, global, query]: "
                                << "[" << storage_schema_version << ", " << global_schema_version << ", " << query_schema_version << "].");
    };
    bool ok;
    {
        std::tie(storage_, lock, storage_schema_version, ok) = get_and_lock_storage(false);
        if (ok)
        {
            log_schema_version("OK, no syncing required.");
            storage = storage_;
            table_lock = lock;
            return;
        }
    }

    /// If first try failed, sync schema and try again.
    {
        log_schema_version("not OK, syncing schemas.");
        auto start_time = Clock::now();
        context.getTMTContext().getSchemaSyncer()->syncSchemas(context);
        auto schema_sync_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
        LOG_DEBUG(log, __PRETTY_FUNCTION__ << " Table " << table_id << " schema sync cost " << schema_sync_cost << "ms.");

        std::tie(storage_, lock, storage_schema_version, ok) = get_and_lock_storage(true);
        if (ok)
        {
            log_schema_version("OK after syncing.");
            storage = storage_;
            table_lock = lock;
            return;
        }

        throw Exception("Shouldn't reach here", ErrorCodes::UNKNOWN_EXCEPTION);
    }
}

SortDescription InterpreterDAG::getSortDescription(Strings & order_column_names)
{
    // construct SortDescription
    SortDescription order_descr;
    const tipb::TopN & topn = dag.getTopN();
    order_descr.reserve(topn.order_by_size());
    for (int i = 0; i < topn.order_by_size(); i++)
    {
        String name = order_column_names[i];
        int direction = topn.order_by(i).desc() ? -1 : 1;
        // MySQL/TiDB treats NULL as "minimum".
        int nulls_direction = -1;
        // todo get this information from DAGRequest
        // currently use the default value
        std::shared_ptr<Collator> collator;

        order_descr.emplace_back(name, direction, nulls_direction, collator);
    }
    return order_descr;
}

void InterpreterDAG::executeUnion(Pipeline & pipeline)
{
    if (pipeline.hasMoreThanOneStream())
    {
        pipeline.firstStream() = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, nullptr, max_streams);
        pipeline.streams.resize(1);
    }
}

void InterpreterDAG::executeOrder(Pipeline & pipeline, Strings & order_column_names)
{
    SortDescription order_descr = getSortDescription(order_column_names);
    const Settings & settings = context.getSettingsRef();
    Int64 limit = dag.getTopN().limit();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(pipeline.firstStream(), order_descr, settings.max_block_size,
        limit, settings.max_bytes_before_external_sort, context.getTemporaryPath());
}

void InterpreterDAG::recordProfileStreams(Pipeline & pipeline, Int32 index)
{
    for (auto & stream : pipeline.streams)
    {
        dag.getDAGContext().profile_streams_list[index].push_back(stream);
    }
}

void InterpreterDAG::executeImpl(Pipeline & pipeline)
{
    executeTS(dag.getTS(), pipeline);
    recordProfileStreams(pipeline, dag.getTSIndex());

    auto res = analyzeExpressions();
    // execute selection
    if (res.has_where)
    {
        executeWhere(pipeline, res.before_where, res.filter_column_name);
        recordProfileStreams(pipeline, dag.getSelectionIndex());
    }
    if (res.need_aggregate)
    {
        // execute aggregation
        executeAggregation(pipeline, res.before_aggregation, res.aggregation_keys, res.aggregate_descriptions);
        recordProfileStreams(pipeline, dag.getAggregationIndex());
    }
    if (res.before_order_and_select)
    {
        executeExpression(pipeline, res.before_order_and_select);
    }

    if (res.has_order_by)
    {
        // execute topN
        executeOrder(pipeline, res.order_column_names);
        recordProfileStreams(pipeline, dag.getTopNIndex());
    }

    // execute projection
    executeFinalProject(pipeline);

    // execute limit
    if (dag.hasLimit() && !dag.hasTopN())
    {
        executeLimit(pipeline);
        recordProfileStreams(pipeline, dag.getLimitIndex());
    }
}

void InterpreterDAG::executeFinalProject(Pipeline & pipeline)
{
    auto columns = pipeline.firstStream()->getHeader();
    NamesAndTypesList input_column;
    for (auto column : columns.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(final_project));
    // add final project
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project); });
}

void InterpreterDAG::executeLimit(Pipeline & pipeline)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, dag.getLimit().limit(), 0, false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline);
        pipeline.transform(
            [&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, dag.getLimit().limit(), 0, false); });
    }
}

BlockIO InterpreterDAG::execute()
{
    Pipeline pipeline;
    executeImpl(pipeline);
    executeUnion(pipeline);

    BlockIO res;
    res.in = pipeline.firstStream();

    LOG_DEBUG(
        log, __PRETTY_FUNCTION__ << " Convert DAG request to BlockIO, adding " << analyzer->getImplicitCastCount() << " implicit cast");
    return res;
}
} // namespace DB

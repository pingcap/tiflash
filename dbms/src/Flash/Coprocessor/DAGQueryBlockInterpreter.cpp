#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/CoprocessorBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LearnerRead.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>

#include "InterpreterDAGHelper.hpp"

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int TOO_MANY_COLUMNS;
extern const int SCHEMA_VERSION_ERROR;
extern const int UNKNOWN_EXCEPTION;
extern const int COP_BAD_DAG_REQUEST;
extern const int NO_COMMON_TYPE;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char region_exception_after_read_from_storage_some_error[];
extern const char region_exception_after_read_from_storage_all_error[];
extern const char pause_after_learner_read[];
} // namespace FailPoints

DAGQueryBlockInterpreter::DAGQueryBlockInterpreter(Context & context_, const std::vector<BlockInputStreams> & input_streams_vec_,
    const DAGQueryBlock & query_block_, bool keep_session_timezone_info_, const tipb::DAGRequest & rqst_, ASTPtr dummy_query_,
    const DAGQuerySource & dag_, std::vector<SubqueriesForSets> & subqueriesForSets_)
    : context(context_),
      input_streams_vec(input_streams_vec_),
      query_block(query_block_),
      keep_session_timezone_info(keep_session_timezone_info_),
      rqst(rqst_),
      dummy_query(std::move(dummy_query_)),
      dag(dag_),
      subqueriesForSets(subqueriesForSets_),
      log(&Logger::get("DAGQueryBlockInterpreter"))
{
    if (query_block.selection != nullptr)
    {
        for (auto & condition : query_block.selection->selection().conditions())
            conditions.push_back(&condition);
    }
    const Settings & settings = context.getSettingsRef();
    if (dag.isBatchCop())
        max_streams = settings.max_threads;
    else
        max_streams = 1;
    if (max_streams > 1)
    {
        max_streams *= settings.max_streams_to_max_threads_ratio;
    }
}

template <typename HandleType>
void constructHandleColRefExpr(tipb::Expr & expr, Int64 col_index)
{
    expr.set_tp(tipb::ExprType::ColumnRef);
    std::stringstream ss;
    encodeDAGInt64(col_index, ss);
    expr.set_val(ss.str());
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeLongLong);
    // handle col is always not null
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    if constexpr (!std::is_signed_v<HandleType>)
    {
        field_type->set_flag(TiDB::ColumnFlagUnsigned);
    }
}

template <typename HandleType>
void constructIntLiteralExpr(tipb::Expr & expr, HandleType value)
{
    constexpr bool is_signed = std::is_signed_v<HandleType>;
    if constexpr (is_signed)
    {
        expr.set_tp(tipb::ExprType::Int64);
    }
    else
    {
        expr.set_tp(tipb::ExprType::Uint64);
    }
    std::stringstream ss;
    if constexpr (is_signed)
    {
        encodeDAGInt64(value, ss);
    }
    else
    {
        encodeDAGUInt64(value, ss);
    }
    expr.set_val(ss.str());
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeLongLong);
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    if constexpr (!is_signed)
    {
        field_type->set_flag(TiDB::ColumnFlagUnsigned);
    }
}

template <typename HandleType>
void constructBoundExpr(Int32 handle_col_id, tipb::Expr & expr, TiKVHandle::Handle<HandleType> bound, bool is_left_bound)
{
    auto * left = expr.add_children();
    constructHandleColRefExpr<HandleType>(*left, handle_col_id);
    auto * right = expr.add_children();
    constructIntLiteralExpr<HandleType>(*right, bound.handle_id);
    expr.set_tp(tipb::ExprType::ScalarFunc);
    if (is_left_bound)
        expr.set_sig(tipb::ScalarFuncSig::GEInt);
    else
        expr.set_sig(tipb::ScalarFuncSig::LTInt);
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeLongLong);
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    field_type->set_flag(TiDB::ColumnFlagUnsigned);
}

template <typename HandleType>
void constructExprBasedOnRange(Int32 handle_col_id, tipb::Expr & expr, HandleRange<HandleType> range)
{
    if (range.second == TiKVHandle::Handle<HandleType>::max)
    {
        constructBoundExpr<HandleType>(handle_col_id, expr, range.first, true);
    }
    else
    {
        auto * left = expr.add_children();
        constructBoundExpr<HandleType>(handle_col_id, *left, range.first, true);
        auto * right = expr.add_children();
        constructBoundExpr<HandleType>(handle_col_id, *right, range.second, false);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.set_sig(tipb::ScalarFuncSig::LogicalAnd);
        auto * field_type = expr.mutable_field_type();
        field_type->set_tp(TiDB::TypeLongLong);
        field_type->set_flag(TiDB::ColumnFlagNotNull);
        field_type->set_flag(TiDB::ColumnFlagUnsigned);
    }
}

template <typename HandleType>
bool checkRangeAndGenExprIfNeeded(std::vector<HandleRange<HandleType>> & ranges, const std::vector<HandleRange<HandleType>> & region_ranges,
    Int32 handle_col_id, tipb::Expr & handle_filter, Logger * log)
{
    if (ranges.empty())
    {
        // generate an always false filter
        LOG_WARNING(log, "income key ranges is empty");
        constructInt64LiteralTiExpr(handle_filter, 0);
        return false;
    }
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
            if (merged_range.second > merged_range.first)
                merged_ranges.emplace_back(std::make_pair(merged_range.first, merged_range.second));
            merged_range.first = ranges[i].first;
            merged_range.second = ranges[i].second;
        }
    }
    if (merged_range.second > merged_range.first)
        merged_ranges.emplace_back(std::make_pair(merged_range.first, merged_range.second));

    bool ret = true;
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
        {
            ret = false;
            break;
        }
    }
    LOG_DEBUG(log, "ret " << ret);
    if (!ret)
    {
        if (merged_ranges.empty())
        {
            constructInt64LiteralTiExpr(handle_filter, 0);
        }
        else if (merged_ranges.size() == 1)
        {
            constructExprBasedOnRange<HandleType>(handle_col_id, handle_filter, merged_ranges[0]);
        }
        else
        {
            for (const auto & range : merged_ranges)
            {
                auto * filter = handle_filter.add_children();
                constructExprBasedOnRange<HandleType>(handle_col_id, *filter, range);
            }
            handle_filter.set_tp(tipb::ExprType::ScalarFunc);
            handle_filter.set_sig(tipb::ScalarFuncSig::LogicalOr);
            auto * field_type = handle_filter.mutable_field_type();
            field_type->set_tp(TiDB::TypeLongLong);
            field_type->set_flag(TiDB::ColumnFlagNotNull);
            field_type->set_flag(TiDB::ColumnFlagUnsigned);
        }
    }
    return ret;
}

bool checkKeyRanges(const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges, TableID table_id, bool pk_is_uint64,
    const ImutRegionRangePtr & region_key_range, Int32 handle_col_id, tipb::Expr & handle_filter, Logger * log)
{
    LOG_INFO(log, "pk_is_uint: " << pk_is_uint64);
    if (key_ranges.empty())
    {
        LOG_WARNING(log, "income key ranges is empty1");
        constructInt64LiteralTiExpr(handle_filter, 0);
        return false;
    }

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
        return checkRangeAndGenExprIfNeeded<UInt64>(update_handle_ranges, update_region_handle_ranges, handle_col_id, handle_filter, log);
    }
    else
        return checkRangeAndGenExprIfNeeded<Int64>(handle_ranges, region_handle_ranges, handle_col_id, handle_filter, log);
}

// the flow is the same as executeFetchcolumns
void DAGQueryBlockInterpreter::executeTS(const tipb::TableScan & ts, Pipeline & pipeline)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest);
    }
    if (dag.getRegions().empty())
    {
        throw TiFlashException("Dag Request does not have region to read. ", Errors::Coprocessor::BadRequest);
    }

    TableID table_id = ts.table_id();

    const Settings & settings = context.getSettingsRef();
    auto & tmt = context.getTMTContext();

    auto mvcc_query_info = std::make_unique<MvccQueryInfo>();
    mvcc_query_info->resolve_locks = true;
    mvcc_query_info->read_tso = settings.read_tso;
    // We need to validate regions snapshot after getting streams from storage.
    LearnerReadSnapshot learner_read_snapshot;
    std::unordered_map<RegionID, const RegionInfo &> region_retry;
    if (!dag.isBatchCop())
    {
        if (auto [info_retry, status] = MakeRegionQueryInfos(dag.getRegions(), {}, tmt, *mvcc_query_info, table_id); info_retry)
            throw RegionException({(*info_retry).begin()->first}, status);

        learner_read_snapshot = doLearnerRead(table_id, *mvcc_query_info, max_streams, tmt, log);
    }
    else
    {
        std::unordered_set<RegionID> force_retry;
        for (;;)
        {
            try
            {
                region_retry.clear();
                auto [retry, status] = MakeRegionQueryInfos(dag.getRegions(), force_retry, tmt, *mvcc_query_info, table_id);
                std::ignore = status;
                if (retry)
                {
                    region_retry = std::move(*retry);
                    for (auto & r : region_retry)
                        force_retry.emplace(r.first);
                }
                if (mvcc_query_info->regions_query_info.empty())
                    break;
                learner_read_snapshot = doLearnerRead(table_id, *mvcc_query_info, max_streams, tmt, log);
                break;
            }
            catch (const LockException & e)
            {
                // We can also use current thread to resolve lock, but it will block next process.
                // So, force this region retry in another thread in CoprocessorBlockInputStream.
                force_retry.emplace(e.region_id);
            }
            catch (const RegionException & e)
            {
                if (tmt.getTerminated())
                    throw TiFlashException("TiFlash server is terminating", Errors::Coprocessor::Internal);
                // By now, RegionException will contain all region id of MvccQueryInfo, which is needed by CHSpark.
                // When meeting RegionException, we can let MakeRegionQueryInfos to check in next loop.
                force_retry.insert(e.unavailable_region.begin(), e.unavailable_region.end());
            }
            catch (DB::Exception & e)
            {
                e.addMessage("(while doing learner read for table, table_id: " + DB::toString(table_id) + ")");
                throw;
            }
        }
    }

    if (settings.schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION)
    {
        storage = context.getTMTContext().getStorages().get(table_id);
        if (storage == nullptr)
        {
            throw TiFlashException("Table " + std::to_string(table_id) + " doesn't exist.", Errors::Table::NotExists);
        }
        table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
    }
    else
    {
        getAndLockStorageWithSchemaVersion(table_id, settings.schema_version);
    }

    Names required_columns;
    std::vector<NameAndTypePair> source_columns;
    std::vector<bool> is_ts_column;
    String handle_column_name = MutableSupport::tidb_pk_column_name;
    if (auto pk_handle_col = storage->getTableInfo().getPKHandleColumn())
        handle_column_name = pk_handle_col->get().name;

    for (Int32 i = 0; i < ts.columns().size(); i++)
    {
        auto const & ci = ts.columns(i);
        ColumnID cid = ci.column_id();

        if (cid == -1)
        {
            // Column ID -1 return the handle column
            required_columns.push_back(handle_column_name);
            auto pair = storage->getColumns().getPhysical(handle_column_name);
            source_columns.push_back(pair);
            is_ts_column.push_back(false);
            continue;
        }

        String name = storage->getTableInfo().getColumnName(cid);
        required_columns.push_back(name);
        auto pair = storage->getColumns().getPhysical(name);
        source_columns.emplace_back(std::move(pair));
        is_ts_column.push_back(ci.tp() == TiDB::TypeTimestamp);
    }

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    if (query_block.aggregation == nullptr)
    {
        if (query_block.isRootQueryBlock())
        {
            for (auto i : query_block.output_offsets)
            {
                if ((size_t)i >= required_columns.size())
                {
                    // array index out of bound
                    throw TiFlashException("Output offset index is out of bound", Errors::Coprocessor::BadRequest);
                }
                // do not have alias
                final_project.emplace_back(required_columns[i], "");
            }
        }
        else
        {
            for (size_t i = 0; i < required_columns.size(); i++)
                /// for child query block, the final project is all the columns read from
                /// the table and add alias start with qb_column_prefix to avoid column name conflict
                final_project.emplace_back(required_columns[i], query_block.qb_column_prefix + required_columns[i]);
        }
    }
    // todo handle alias column
    if (settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
    {
        throw TiFlashException("Limit for number of columns to read exceeded. "
                               "Requested: "
                + toString(required_columns.size()) + ", maximum: " + settings.max_columns_to_read.toString(),
            Errors::BroadcastJoin::TooManyColumns);
    }

    size_t max_block_size = settings.max_block_size;

    SelectQueryInfo query_info;
    /// to avoid null point exception
    query_info.query = dummy_query;
    query_info.dag_query = std::make_unique<DAGQueryInfo>(conditions, analyzer->getPreparedSets(), analyzer->getCurrentInputColumns());
    query_info.mvcc_query_info = std::move(mvcc_query_info);

    FAIL_POINT_PAUSE(FailPoints::pause_after_learner_read);
    bool need_local_read = !query_info.mvcc_query_info->regions_query_info.empty();
    if (need_local_read)
    {
        readFromLocalStorage(table_id, required_columns, query_info, max_block_size, learner_read_snapshot, pipeline, region_retry);
    }

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop mode.
    if (!region_retry.empty())
    {
        LOG_DEBUG(log, ({
            std::stringstream ss;
            ss << "Start to retry " << region_retry.size() << " regions (";
            for (auto & r : region_retry)
                ss << r.first << ",";
            ss << ")";
            ss.str();
        }));

        DAGSchema schema;
        ::tipb::DAGRequest dag_req;

        {
            const auto & table_info = storage->getTableInfo();
            tipb::Executor * ts_exec = dag_req.add_executors();
            ts_exec->set_tp(tipb::ExecType::TypeTableScan);
            *(ts_exec->mutable_tbl_scan()) = ts;

            for (int i = 0; i < ts.columns().size(); ++i)
            {
                const auto & col = ts.columns(i);
                auto col_id = col.column_id();

                if (col_id == DB::TiDBPkColumnID)
                {
                    ColumnInfo ci;
                    ci.tp = TiDB::TypeLongLong;
                    ci.setPriKeyFlag();
                    ci.setNotNullFlag();
                    schema.emplace_back(std::make_pair(handle_column_name, std::move(ci)));
                }
                else
                {
                    auto & col_info = table_info.getColumnInfo(col_id);
                    schema.emplace_back(std::make_pair(col_info.name, col_info));
                }
                dag_req.add_output_offsets(i);
            }
            dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
        }

        std::vector<pingcap::coprocessor::KeyRange> ranges;
        for (auto & info : region_retry)
        {
            for (auto & range : info.second.key_ranges)
                ranges.emplace_back(range.first, range.second);
        }
        sort(ranges.begin(), ranges.end());
        executeRemoteQueryImpl(pipeline, ranges, dag_req, schema);
    }

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

    if (addTimeZoneCastAfterTS(is_ts_column, pipeline) && !query_block.aggregation
        && (keep_session_timezone_info || !query_block.isRootQueryBlock()))
    {
        if (query_block.isRootQueryBlock())
        {
            for (size_t i = 0; i < query_block.output_offsets.size(); i++)
            {
                int column_index = query_block.output_offsets[i];
                if (is_ts_column[column_index])
                    final_project[i].first = analyzer->getCurrentInputColumns()[column_index].name;
            }
        }
        else
        {
            for (size_t i = 0; i < final_project.size(); i++)
            {
                if (is_ts_column[i])
                    final_project[i].first = analyzer->getCurrentInputColumns()[i].name;
            }
        }
    }
}

void DAGQueryBlockInterpreter::readFromLocalStorage( //
    const TableID table_id, const Names & required_columns, SelectQueryInfo & query_info, const size_t max_block_size,
    const LearnerReadSnapshot & learner_read_snapshot, //
    Pipeline & pipeline, std::unordered_map<RegionID, const RegionInfo &> & region_retry)
{
    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
    auto & tmt = context.getTMTContext();
    // TODO: Note that if storage is (Txn)MergeTree, and any region exception thrown, we won't do retry here.
    // Now we only support DeltaTree in production environment and don't do any extra check for storage type here.

    int num_allow_retry = 1;
    while (true)
    {
        try
        {
            pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams);

            // After getting streams from storage, we need to validate whether regions have changed or not after learner read.
            // In case the versions of regions have changed, those `streams` may contain different data other than expected.
            // Like after region merge/split.

            // Inject failpoint to throw RegionException
            fiu_do_on(FailPoints::region_exception_after_read_from_storage_some_error, {
                const auto & regions_info = query_info.mvcc_query_info->regions_query_info;
                RegionException::UnavailableRegions region_ids;
                for (const auto & info : regions_info)
                {
                    if (rand() % 100 > 50)
                        region_ids.insert(info.region_id);
                }
                throw RegionException(std::move(region_ids), RegionException::RegionReadStatus::NOT_FOUND);
            });
            fiu_do_on(FailPoints::region_exception_after_read_from_storage_all_error, {
                const auto & regions_info = query_info.mvcc_query_info->regions_query_info;
                RegionException::UnavailableRegions region_ids;
                for (const auto & info : regions_info)
                    region_ids.insert(info.region_id);
                throw RegionException(std::move(region_ids), RegionException::RegionReadStatus::NOT_FOUND);
            });
            validateQueryInfo(*query_info.mvcc_query_info, learner_read_snapshot, tmt, log);
            break;
        }
        catch (RegionException & e)
        {
            /// Recover from region exception when super batch is enable
            if (dag.isBatchCop())
            {
                // clean all streams from local because we are not sure the correctness of those streams
                pipeline.streams.clear();
                const auto & dag_regions = dag.getRegions();
                std::stringstream ss;
                // Normally there is only few regions need to retry when super batch is enabled. Retry to read
                // from local first. However, too many retry in different places may make the whole process
                // time out of control. We limit the number of retries to 1 now.
                if (likely(num_allow_retry > 0))
                {
                    --num_allow_retry;
                    auto & regions_query_info = query_info.mvcc_query_info->regions_query_info;
                    for (auto iter = regions_query_info.begin(); iter != regions_query_info.end(); /**/)
                    {
                        if (e.unavailable_region.find(iter->region_id) != e.unavailable_region.end())
                        {
                            // move the error regions info from `query_info.mvcc_query_info->regions_query_info` to `region_retry`
                            if (auto region_iter = dag_regions.find(iter->region_id); likely(region_iter != dag_regions.end()))
                            {
                                region_retry.emplace(region_iter->first, region_iter->second);
                                ss << region_iter->first << ",";
                            }
                            iter = regions_query_info.erase(iter);
                        }
                        else
                        {
                            ++iter;
                        }
                    }
                    LOG_WARNING(log,
                        "RegionException after read from storage, regions ["
                            << ss.str() << "], message: " << e.message()
                            << (regions_query_info.empty() ? "" : ", retry to read from local"));
                    if (unlikely(regions_query_info.empty()))
                        break; // no available region in local, break retry loop
                    continue;  // continue to retry read from local storage
                }
                else
                {
                    // push all regions to `region_retry` to retry from other tiflash nodes
                    for (const auto & region : query_info.mvcc_query_info->regions_query_info)
                    {
                        auto iter = dag_regions.find(region.region_id);
                        if (likely(iter != dag_regions.end()))
                        {
                            region_retry.emplace(iter->first, iter->second);
                            ss << iter->first << ",";
                        }
                    }
                    LOG_WARNING(log, "RegionException after read from storage, regions [" << ss.str() << "], message: " << e.message());
                    break; // break retry loop
                }
            }
            else
            {
                // Throw an exception for TiDB / TiSpark to retry
                e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                    + "`, table_id: " + DB::toString(table_id) + ")");
                throw;
            }
        }
        catch (DB::Exception & e)
        {
            /// Other unknown exceptions
            e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                + "`, table_id: " + DB::toString(table_id) + ")");
            throw;
        }
    }
}

void DAGQueryBlockInterpreter::prepareJoinKeys(const google::protobuf::RepeatedPtrField<tipb::Expr> & keys, const DataTypes & key_types,
    Pipeline & pipeline, Names & key_names, bool left, bool is_right_out_join)
{
    std::vector<NameAndTypePair> source_columns;
    for (auto const & p : pipeline.firstStream()->getHeader().getNamesAndTypesList())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    if (dag_analyzer.appendJoinKey(chain, keys, key_types, key_names, left, is_right_out_join))
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions()); });
    }
}

/// ClickHouse require join key to be exactly the same type
/// TiDB only require the join key to be the same category
/// for example decimal(10,2) join decimal(20,0) is allowed in
/// TiDB and will throw exception in ClickHouse
void getJoinKeyTypes(const tipb::Join & join, DataTypes & key_types)
{
    for (int i = 0; i < join.left_join_keys().size(); i++)
    {
        if (!exprHasValidFieldType(join.left_join_keys(i)) || !exprHasValidFieldType(join.right_join_keys(i)))
            throw TiFlashException("Join key without field type", Errors::Coprocessor::BadRequest);
        DataTypes types;
        types.emplace_back(getDataTypeByFieldType(join.left_join_keys(i).field_type()));
        types.emplace_back(getDataTypeByFieldType(join.right_join_keys(i).field_type()));
        DataTypePtr common_type = getLeastSupertype(types);
        key_types.emplace_back(common_type);
    }
}

void DAGQueryBlockInterpreter::executeJoin(const tipb::Join & join, Pipeline & pipeline, SubqueryForSet & right_query)
{
    // build
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Inner}, {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Right}};
    if (input_streams_vec.size() != 2)
    {
        throw TiFlashException("Join query block must have 2 input streams", Errors::BroadcastJoin::Internal);
    }

    auto join_type_it = join_type_map.find(join.join_type());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type in dag request", Errors::Coprocessor::BadRequest);
    ASTTableJoin::Kind kind = join_type_it->second;

    BlockInputStreams left_streams;
    BlockInputStreams right_streams;
    Names left_key_names;
    Names right_key_names;
    if (join.inner_idx() == 0)
    {
        // in DAG request, inner part is the build side, however for tiflash implementation,
        // the build side must be the right side, so need to update the join type if needed
        if (kind == ASTTableJoin::Kind::Left)
            kind = ASTTableJoin::Kind::Right;
        else if (kind == ASTTableJoin::Kind::Right)
            kind = ASTTableJoin::Kind::Left;
        left_streams = input_streams_vec[1];
        right_streams = input_streams_vec[0];
    }
    else
    {
        left_streams = input_streams_vec[0];
        right_streams = input_streams_vec[1];
    }

    if (kind == ASTTableJoin::Kind::Full)
    {
        throw TiFlashException("Only Inner/Left/Right join is supported", Errors::Coprocessor::Unimplemented);
    }

    std::vector<NameAndTypePair> join_output_columns;
    for (auto const & p : input_streams_vec[0][0]->getHeader().getNamesAndTypesList())
    {
        join_output_columns.emplace_back(p.name, p.type);
    }
    for (auto const & p : input_streams_vec[1][0]->getHeader().getNamesAndTypesList())
    {
        join_output_columns.emplace_back(p.name, p.type);
    }
    /// all the columns from right table should be added after join, even for the join key
    NamesAndTypesList columns_added_by_join;
    for (auto const & p : right_streams[0]->getHeader().getNamesAndTypesList())
    {
        columns_added_by_join.emplace_back(p.name, p.type);
    }

    if (!query_block.aggregation)
    {
        for (auto const & p : input_streams_vec[0][0]->getHeader().getNamesAndTypesList())
            final_project.emplace_back(p.name, query_block.qb_column_prefix + p.name);
        for (auto const & p : input_streams_vec[1][0]->getHeader().getNamesAndTypesList())
            final_project.emplace_back(p.name, query_block.qb_column_prefix + p.name);
    }

    DataTypes join_key_types;
    getJoinKeyTypes(join, join_key_types);
    TiDB::TiDBCollators collators;
    size_t join_key_size = join_key_types.size();
    if (join.probe_types_size() == static_cast<int>(join_key_size) && join.build_types_size() == join.probe_types_size())
        for (size_t i = 0; i < join_key_size; i++)
        {
            if (removeNullable(join_key_types[i])->isString())
            {
                if (join.probe_types(i).collate() != join.build_types(i).collate())
                    throw TiFlashException("Join with different collators on the join key", Errors::Coprocessor::BadRequest);
                collators.push_back(getCollatorFromFieldType(join.probe_types(i)));
            }
            else
                collators.push_back(nullptr);
        }

    /// add necessary transformation if the join key is an expression
    Pipeline left_pipeline;
    left_pipeline.streams = left_streams;
    prepareJoinKeys(join.inner_idx() == 0 ? join.right_join_keys() : join.left_join_keys(), join_key_types, left_pipeline, left_key_names,
        true, kind == ASTTableJoin::Kind::Right);
    Pipeline right_pipeline;
    right_pipeline.streams = right_streams;
    prepareJoinKeys(join.inner_idx() == 0 ? join.left_join_keys() : join.right_join_keys(), join_key_types, right_pipeline, right_key_names,
        false, kind == ASTTableJoin::Kind::Right);

    left_streams = left_pipeline.streams;
    right_streams = right_pipeline.streams;

    const Settings & settings = context.getSettingsRef();
    JoinPtr joinPtr = std::make_shared<Join>(left_key_names, right_key_names, true,
        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode), kind, ASTTableJoin::Strictness::All,
        collators);
    executeUnion(right_pipeline, max_streams);
    right_query.source = right_pipeline.firstStream();
    right_query.join = joinPtr;
    right_query.join->setSampleBlock(right_query.source->getHeader());
    dag.getDAGContext().getProfileStreamsMapForJoinBuildSide()[query_block.qb_join_subquery_alias].push_back(right_query.source);

    std::vector<NameAndTypePair> source_columns;
    for (const auto & p : left_streams[0]->getHeader().getNamesAndTypesList())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    dag_analyzer.appendJoin(chain, right_query, columns_added_by_join);
    pipeline.streams = left_streams;
    /// add join input stream
    if (kind == ASTTableJoin::Kind::Full || kind == ASTTableJoin::Kind::Right)
        pipeline.stream_with_non_joined_data = chain.getLastActions()->createStreamWithNonJoinedDataIfFullOrRightJoin(
            pipeline.firstStream()->getHeader(), settings.max_block_size);
    for (auto & stream : pipeline.streams)
        stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions());

    // todo should add a project here???
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(join_output_columns), context);
}

// add timezone cast for timestamp type, this is used to support session level timezone
bool DAGQueryBlockInterpreter::addTimeZoneCastAfterTS(std::vector<bool> & is_ts_column, Pipeline & pipeline)
{
    bool hasTSColumn = false;
    for (auto b : is_ts_column)
        hasTSColumn |= b;
    if (!hasTSColumn)
        return false;

    ExpressionActionsChain chain;
    /// only keep UTC column if
    /// 1. the query block is the root query block
    /// 2. keep_session_timezone_info is false
    /// 3. current query block does not have aggregation
    if (analyzer->appendTimeZoneCastsAfterTS(
            chain, is_ts_column, query_block.isRootQueryBlock() && !keep_session_timezone_info && query_block.aggregation == nullptr))
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions()); });
        return true;
    }
    else
        return false;
}

AnalysisResult DAGQueryBlockInterpreter::analyzeExpressions()
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    if (!conditions.empty())
    {
        analyzer->appendWhere(chain, conditions, res.filter_column_name);
        res.has_where = true;
        res.before_where = chain.getLastActions();
        chain.addStep();
    }
    // There will be either Agg...
    if (query_block.aggregation)
    {
        /// collation sensitive group by is slower then normal group by, use normal group by by default
        // todo better to let TiDB decide whether group by is collation sensitive or not
        analyzer->appendAggregation(chain, query_block.aggregation->aggregation(), res.aggregation_keys, res.aggregation_collators,
            res.aggregate_descriptions, context.getSettingsRef().group_by_collation_sensitive);
        res.need_aggregate = true;
        res.before_aggregation = chain.getLastActions();

        chain.finalize();
        chain.clear();

        // add cast if type is not match
        analyzer->appendAggSelect(
            chain, query_block.aggregation->aggregation(), keep_session_timezone_info || !query_block.isRootQueryBlock());
        if (query_block.isRootQueryBlock())
        {
            // todo for root query block, use output offsets to reconstruct the final project
            for (auto & element : analyzer->getCurrentInputColumns())
            {
                final_project.emplace_back(element.name, "");
            }
        }
        else
        {
            for (auto & element : analyzer->getCurrentInputColumns())
            {
                final_project.emplace_back(element.name, query_block.qb_column_prefix + element.name);
            }
        }
    }
    // Or TopN, not both.
    if (query_block.limitOrTopN && query_block.limitOrTopN->tp() == tipb::ExecType::TypeTopN)
    {
        res.has_order_by = true;
        analyzer->appendOrderBy(chain, query_block.limitOrTopN->topn(), res.order_columns);
    }
    // Append final project results if needed.
    analyzer->appendFinalProject(chain, final_project);
    res.before_order_and_select = chain.getLastActions();
    chain.finalize();
    chain.clear();
    //todo need call prependProjectInput??
    return res;
}

void DAGQueryBlockInterpreter::executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expr, String & filter_column)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column); });
}

void DAGQueryBlockInterpreter::executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expr, Names & key_names,
    TiDB::TiDBCollators & collators, AggregateDescriptions & aggregates)
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
    bool has_collator = false;
    for (auto & p : collators)
    {
        if (p != nullptr)
        {
            has_collator = true;
            break;
        }
    }

    Aggregator::Params params(header, keys, aggregates, false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
        settings.compile && !has_collator ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set, context.getTemporaryPath(),
        has_collator ? collators : TiDB::dummy_collators);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(pipeline.streams,
            pipeline.stream_with_non_joined_data, params, context.getFileProvider(), true, max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                                                                : static_cast<size_t>(settings.max_threads));

        pipeline.stream_with_non_joined_data = nullptr;
        pipeline.streams.resize(1);
    }
    else
    {
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);
        if (pipeline.stream_with_non_joined_data)
            inputs.push_back(pipeline.stream_with_non_joined_data);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            std::make_shared<ConcatBlockInputStream>(inputs), params, context.getFileProvider(), true);
        pipeline.stream_with_non_joined_data = nullptr;
    }
    // add cast
}

void DAGQueryBlockInterpreter::executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr); });
    }
}

void DAGQueryBlockInterpreter::getAndLockStorageWithSchemaVersion(TableID table_id, Int64 query_schema_version)
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
                throw TiFlashException("Table " + std::to_string(table_id) + " doesn't exist.", Errors::Table::NotExists);
            else
                return std::make_tuple(nullptr, nullptr, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, false);
        }

        if (storage_->engineType() != ::TiDB::StorageEngine::TMT && storage_->engineType() != ::TiDB::StorageEngine::DT)
        {
            throw TiFlashException("Specifying schema_version for non-managed storage: " + storage_->getName()
                    + ", table: " + storage_->getTableName() + ", id: " + DB::toString(table_id) + " is not allowed",
                Errors::Coprocessor::Internal);
        }

        /// Lock storage.
        auto lock = storage_->lockStructure(false, __PRETTY_FUNCTION__);

        /// Check schema version, requiring TiDB/TiSpark and TiFlash both use exactly the same schema.
        // We have three schema versions, two in TiFlash:
        // 1. Storage: the version that this TiFlash table (storage) was last altered.
        // 2. Global: the version that TiFlash global schema is at.
        // And one from TiDB/TiSpark:
        // 3. Query: the version that TiDB/TiSpark used for this query.
        auto storage_schema_version = storage_->getTableInfo().schema_version;
        // Not allow storage > query in any case, one example is time travel queries.
        if (storage_schema_version > query_schema_version)
            throw TiFlashException("Table " + std::to_string(table_id) + " schema version " + std::to_string(storage_schema_version)
                    + " newer than query schema version " + std::to_string(query_schema_version),
                Errors::Table::SchemaVersionError);
        // From now on we have storage <= query.
        // If schema was synced, it implies that global >= query, as mentioned above we have storage <= query, we are OK to serve.
        if (schema_synced)
            return std::make_tuple(storage_, lock, storage_schema_version, true);
        // From now on the schema was not synced.
        // 1. storage == query, TiDB/TiSpark is using exactly the same schema that altered this table, we are just OK to serve.
        // 2. global >= query, TiDB/TiSpark is using a schema older than TiFlash global, but as mentioned above we have storage <= query,
        // meaning that the query schema is still newer than the time when this table was last altered, so we still OK to serve.
        if (storage_schema_version == query_schema_version || global_schema_version >= query_schema_version)
            return std::make_tuple(storage_, lock, storage_schema_version, true);
        // From now on we have global < query.
        // Return false for outer to sync and retry.
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
        GET_METRIC(context.getTiFlashMetrics(), tiflash_schema_trigger_count, type_cop_read).Increment();
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

        throw TiFlashException("Shouldn't reach here", Errors::Coprocessor::Internal);
    }
}

SortDescription DAGQueryBlockInterpreter::getSortDescription(std::vector<NameAndTypePair> & order_columns)
{
    // construct SortDescription
    SortDescription order_descr;
    const tipb::TopN & topn = query_block.limitOrTopN->topn();
    order_descr.reserve(topn.order_by_size());
    for (int i = 0; i < topn.order_by_size(); i++)
    {
        const auto & name = order_columns[i].name;
        int direction = topn.order_by(i).desc() ? -1 : 1;
        // MySQL/TiDB treats NULL as "minimum".
        int nulls_direction = -1;
        std::shared_ptr<ICollator> collator = nullptr;
        if (removeNullable(order_columns[i].type)->isString())
            collator = getCollatorFromExpr(topn.order_by(i).expr());

        order_descr.emplace_back(name, direction, nulls_direction, collator);
    }
    return order_descr;
}

void DAGQueryBlockInterpreter::executeUnion(Pipeline & pipeline, size_t max_streams)
{
    if (pipeline.hasMoreThanOneStream())
    {
        pipeline.firstStream()
            = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, pipeline.stream_with_non_joined_data, max_streams);
        pipeline.stream_with_non_joined_data = nullptr;
        pipeline.streams.resize(1);
    }
    else if (pipeline.stream_with_non_joined_data)
    {
        pipeline.streams.push_back(pipeline.stream_with_non_joined_data);
        pipeline.stream_with_non_joined_data = nullptr;
    }
}

void DAGQueryBlockInterpreter::executeOrder(Pipeline & pipeline, std::vector<NameAndTypePair> & order_columns)
{
    SortDescription order_descr = getSortDescription(order_columns);
    const Settings & settings = context.getSettingsRef();
    Int64 limit = query_block.limitOrTopN->topn().limit();

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
    executeUnion(pipeline, max_streams);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(pipeline.firstStream(), order_descr, settings.max_block_size,
        limit, settings.max_bytes_before_external_sort, context.getTemporaryPath());
}

void DAGQueryBlockInterpreter::recordProfileStreams(Pipeline & pipeline, const String & key)
{
    dag.getDAGContext().getProfileStreamsMap()[key].qb_id = query_block.id;
    for (auto & stream : pipeline.streams)
    {
        dag.getDAGContext().getProfileStreamsMap()[key].input_streams.push_back(stream);
    }
    if (pipeline.stream_with_non_joined_data)
        dag.getDAGContext().getProfileStreamsMap()[key].input_streams.push_back(pipeline.stream_with_non_joined_data);
}

void copyExecutorTreeWithLocalTableScan(
    tipb::DAGRequest & dag_req, const tipb::Executor * root, tipb::EncodeType encode_type, const tipb::DAGRequest & org_req)
{
    const tipb::Executor * current = root;
    auto * exec = dag_req.mutable_root_executor();
    int exec_id = 0;
    while (current->tp() != tipb::ExecType::TypeTableScan)
    {
        if (current->tp() == tipb::ExecType::TypeSelection)
        {
            exec->set_tp(tipb::ExecType::TypeSelection);
            exec->set_executor_id("selection_" + std::to_string(exec_id));
            auto * sel = exec->mutable_selection();
            for (auto const & condition : current->selection().conditions())
            {
                auto * tmp = sel->add_conditions();
                tmp->CopyFrom(condition);
            }
            exec = sel->mutable_child();
            current = &current->selection().child();
        }
        else if (current->tp() == tipb::ExecType::TypeAggregation || current->tp() == tipb::ExecType::TypeStreamAgg)
        {
            exec->set_tp(current->tp());
            exec->set_executor_id("aggregation_" + std::to_string(exec_id));
            auto * agg = exec->mutable_aggregation();
            for (auto const & expr : current->aggregation().agg_func())
            {
                auto * tmp = agg->add_agg_func();
                tmp->CopyFrom(expr);
            }
            for (auto const & expr : current->aggregation().group_by())
            {
                auto * tmp = agg->add_group_by();
                tmp->CopyFrom(expr);
            }
            agg->set_streamed(current->aggregation().streamed());
            exec = agg->mutable_child();
            current = &current->aggregation().child();
        }
        else if (current->tp() == tipb::ExecType::TypeLimit)
        {
            exec->set_tp(current->tp());
            exec->set_executor_id("limit_" + std::to_string(exec_id));
            auto * limit = exec->mutable_limit();
            limit->set_limit(current->limit().limit());
            exec = limit->mutable_child();
            current = &current->limit().child();
        }
        else if (current->tp() == tipb::ExecType::TypeTopN)
        {
            exec->set_tp(current->tp());
            exec->set_executor_id("topN_" + std::to_string(exec_id));
            auto * topn = exec->mutable_topn();
            topn->set_limit(current->topn().limit());
            for (auto const & expr : current->topn().order_by())
            {
                auto * tmp = topn->add_order_by();
                tmp->CopyFrom(expr);
            }
            exec = topn->mutable_child();
            current = &current->topn().child();
        }
        else
        {
            throw TiFlashException("Not supported yet", Errors::Coprocessor::Unimplemented);
        }
        exec_id++;
    }

    if (current->tp() != tipb::ExecType::TypeTableScan)
        throw TiFlashException("Only support copy from table scan sourced query block", Errors::Coprocessor::Internal);
    exec->set_tp(tipb::ExecType::TypeTableScan);
    exec->set_executor_id("tablescan_" + std::to_string(exec_id));
    auto * new_ts = new tipb::TableScan(current->tbl_scan());
    new_ts->set_next_read_engine(tipb::EngineType::Local);
    exec->set_allocated_tbl_scan(new_ts);

    dag_req.set_encode_type(encode_type);
    if (org_req.has_time_zone_name() && org_req.time_zone_name().length() > 0)
        dag_req.set_time_zone_name(org_req.time_zone_name());
    else if (org_req.has_time_zone_offset())
        dag_req.set_time_zone_offset(org_req.time_zone_offset());
}

void DAGQueryBlockInterpreter::executeRemoteQuery(Pipeline & pipeline)
{
    // remote query containing agg/limit/topN can not running
    // in parellel, but current remote query is running in
    // parellel, so just disable this corner case.
    if (query_block.aggregation || query_block.limitOrTopN)
        throw TiFlashException("Remote query containing agg or limit or topN is not supported", Errors::Coprocessor::BadRequest);
    const auto & ts = query_block.source->tbl_scan();
    std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
    for (auto & range : ts.ranges())
    {
        std::string start_key(range.low());
        DecodedTiKVKey start(std::move(start_key));
        std::string end_key(range.high());
        DecodedTiKVKey end(std::move(end_key));
        key_ranges.emplace_back(std::make_pair(std::move(start), std::move(end)));
    }
    std::vector<pingcap::coprocessor::KeyRange> cop_key_ranges;
    cop_key_ranges.reserve(key_ranges.size());
    for (const auto & key_range : key_ranges)
    {
        cop_key_ranges.emplace_back(static_cast<String>(key_range.first), static_cast<String>(key_range.second));
    }
    sort(cop_key_ranges.begin(), cop_key_ranges.end());

    ::tipb::DAGRequest dag_req;

    tipb::EncodeType encode_type;
    if (!isUnsupportedEncodeType(query_block.output_field_types, tipb::EncodeType::TypeCHBlock))
        encode_type = tipb::EncodeType::TypeCHBlock;
    else if (!isUnsupportedEncodeType(query_block.output_field_types, tipb::EncodeType::TypeChunk))
        encode_type = tipb::EncodeType::TypeChunk;
    else
        encode_type = tipb::EncodeType::TypeDefault;

    copyExecutorTreeWithLocalTableScan(dag_req, query_block.root, encode_type, rqst);
    DAGSchema schema;
    ColumnsWithTypeAndName columns;
    std::vector<bool> is_ts_column;
    std::vector<NameAndTypePair> source_columns;
    for (int i = 0; i < (int)query_block.output_field_types.size(); i++)
    {
        dag_req.add_output_offsets(i);
        ColumnInfo info = fieldTypeToColumnInfo(query_block.output_field_types[i]);
        String col_name = query_block.qb_column_prefix + "col_" + std::to_string(i);
        schema.push_back(std::make_pair(col_name, info));
        is_ts_column.push_back(query_block.output_field_types[i].tp() == TiDB::TypeTimestamp);
        source_columns.emplace_back(col_name, getDataTypeByFieldType(query_block.output_field_types[i]));
        final_project.emplace_back(col_name, "");
    }

    executeRemoteQueryImpl(pipeline, cop_key_ranges, dag_req, schema);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    bool need_append_final_project = false;
    if (encode_type == tipb::EncodeType::TypeDefault)
    {
        /// if the encode type is default, the timestamp column in dag response is UTC based
        /// so need to cast the timezone
        if (addTimeZoneCastAfterTS(is_ts_column, pipeline))
        {
            for (size_t i = 0; i < final_project.size(); i++)
            {
                if (is_ts_column[i])
                    final_project[i].first = analyzer->getCurrentInputColumns()[i].name;
            }
            need_append_final_project = true;
        }
    }

    /// For simplicity, remote subquery only record the CopBlockInputStream time, for example,
    /// if the remote subquery is TS, then it's execute time is the execute time of CopBlockInputStream,
    /// if the remote subquery is TS SEL, then both TS and SEL's execute time is the same as the CopBlockInputStream
    recordProfileStreams(pipeline, query_block.source_name);
    if (query_block.selection)
        recordProfileStreams(pipeline, query_block.selection_name);

    if (need_append_final_project)
        executeFinalProject(pipeline);
}

void DAGQueryBlockInterpreter::executeRemoteQueryImpl(Pipeline & pipeline,
    const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges, ::tipb::DAGRequest & dag_req, const DAGSchema & schema)
{

    pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
    dag_req.SerializeToString(&(req->data));
    req->tp = pingcap::coprocessor::ReqType::DAG;
    req->start_ts = context.getSettingsRef().read_tso;

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
    auto all_tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, cop_key_ranges, req, store_type, &Logger::get("pingcap/coprocessor"));

    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    for (size_t i = 0, task_start = 0; i < concurrent_num; i++)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            task_end++;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::copTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(cluster, tasks, schema, 1);
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void DAGQueryBlockInterpreter::executeImpl(Pipeline & pipeline)
{
    if (query_block.isRemoteQuery())
    {
        executeRemoteQuery(pipeline);
        return;
    }
    SubqueryForSet right_query;
    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        executeJoin(query_block.source->join(), pipeline, right_query);
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else
    {
        executeTS(query_block.source->tbl_scan(), pipeline);
        recordProfileStreams(pipeline, query_block.source_name);
    }

    auto res = analyzeExpressions();
    // execute selection
    if (res.has_where)
    {
        executeWhere(pipeline, res.before_where, res.filter_column_name);
        recordProfileStreams(pipeline, query_block.selection_name);
    }
    LOG_INFO(log,
        "execution stream size for query block(before aggregation) " << query_block.qb_column_prefix << " is " << pipeline.streams.size());
    dag.getDAGContext().final_concurency = pipeline.streams.size();
    if (res.need_aggregate)
    {
        // execute aggregation
        executeAggregation(pipeline, res.before_aggregation, res.aggregation_keys, res.aggregation_collators, res.aggregate_descriptions);
        recordProfileStreams(pipeline, query_block.aggregation_name);
    }
    if (res.before_order_and_select)
    {
        executeExpression(pipeline, res.before_order_and_select);
    }

    if (res.has_order_by)
    {
        // execute topN
        executeOrder(pipeline, res.order_columns);
        recordProfileStreams(pipeline, query_block.limitOrTopN_name);
    }

    // execute projection
    executeFinalProject(pipeline);

    // execute limit
    if (query_block.limitOrTopN != nullptr && query_block.limitOrTopN->tp() == tipb::TypeLimit)
    {
        executeLimit(pipeline);
        recordProfileStreams(pipeline, query_block.limitOrTopN_name);
    }

    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        SubqueriesForSets subquries;
        subquries[query_block.qb_join_subquery_alias] = right_query;
        subqueriesForSets.emplace_back(subquries);
    }
}

void DAGQueryBlockInterpreter::executeFinalProject(Pipeline & pipeline)
{
    auto columns = pipeline.firstStream()->getHeader();
    NamesAndTypesList input_column;
    for (auto & column : columns.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(final_project));
    // add final project
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project); });
}

void DAGQueryBlockInterpreter::executeLimit(Pipeline & pipeline)
{
    size_t limit = 0;
    if (query_block.limitOrTopN->tp() == tipb::TypeLimit)
        limit = query_block.limitOrTopN->limit().limit();
    else
        limit = query_block.limitOrTopN->topn().limit();
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams);
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, false); });
    }
}

BlockInputStreams DAGQueryBlockInterpreter::execute()
{
    Pipeline pipeline;
    executeImpl(pipeline);
    if (pipeline.stream_with_non_joined_data)
        // todo return pipeline instead of BlockInputStreams so we can keep concurrent execution
        executeUnion(pipeline, max_streams);

    return pipeline.streams;
}
} // namespace DB

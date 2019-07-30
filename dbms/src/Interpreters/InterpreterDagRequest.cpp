#include <DataStreams/BlockIO.h>
#include <Interpreters/InterpreterDagRequest.h>
#include <Storages/Transaction/Types.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Region.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/RegionQueryInfo.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Interpreters/CoprocessorBuilderUtils.h>

namespace DB {

    namespace ErrorCodes
    {
        extern const int TOO_MANY_COLUMNS;
    }

    InterpreterDagRequest::InterpreterDagRequest(CoprocessorContext & context_, const tipb::DAGRequest & dag_request_)
    : context(context_), dag_request(dag_request_) {
        for(const tipb::Executor & executor : dag_request.executors()) {
            switch (executor.tp()) {
                case tipb::ExecType::TypeSelection:
                    has_where = true;
                    break;
                case tipb::ExecType::TypeStreamAgg:
                case tipb::ExecType::TypeAggregation:
                    has_agg = true;
                    break;
                case tipb::ExecType::TypeTopN:
                    has_orderby = true;
                case tipb::ExecType::TypeLimit:
                    has_limit = true;
                    break;
                default:
                    break;
            }
        }
    }

    bool InterpreterDagRequest::buildTSPlan(const tipb::TableScan & ts, Pipeline & pipeline) {
        if(!ts.has_table_id()) {
            // do not have table id
            return false;
        }
        TableID id = ts.table_id();
        auto & tmt_ctx = context.ch_context.getTMTContext();
        auto storage = tmt_ctx.getStorages().get(id);
        if(storage == nullptr) {
            tmt_ctx.getSchemaSyncer()->syncSchema(id, context.ch_context, false);
            storage = tmt_ctx.getStorages().get(id);
        }
        if(storage == nullptr) {
            return false;
        }
        auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
        const auto * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
        if(!merge_tree) {
            return false;
        }

        Names required_columns;
        for(const tipb::ColumnInfo & ci : ts.columns()) {
            ColumnID cid = ci.column_id();
            if(cid < 1 || cid > (Int64)merge_tree->getTableInfo().columns.size()) {
                // cid out of bound
                return false;
            }
            String name = merge_tree->getTableInfo().columns[cid - 1].name;
            //todo handle output_offset
            required_columns.push_back(name);
        }
        if(required_columns.empty()) {
            // no column selected, must be something wrong
            return false;
        }

        if(!has_agg) {
            // if the dag request does not contain agg, then the final output is
            // based on the output of table scan
            for (auto i : dag_request.output_offsets()) {
                if (i < 0 || i >= required_columns.size()) {
                    // array index out of bound
                    return false;
                }
                // do not have alias
                final_project.emplace_back(required_columns[i], "");
            }
        }
        // todo handle alias column
        const Settings & settings = context.ch_context.getSettingsRef();

        if(settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read) {
            throw Exception("Limit for number of columns to read exceeded. "
                            "Requested: " + toString(required_columns.size())
                            + ", maximum: " + settings.max_columns_to_read.toString(),
                            ErrorCodes::TOO_MANY_COLUMNS);
        }

        size_t max_block_size = settings.max_block_size;
        size_t max_streams = settings.max_threads;
        QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
        if(max_streams > 1) {
            max_streams *= settings.max_streams_to_max_threads_ratio;
        }

        //todo support index in
        SelectQueryInfo query_info;
        query_info.query = std::make_unique<ASTSelectQuery>();
        ((ASTSelectQuery*)query_info.query.get())->is_fake_sel = true;
        query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>();
        query_info.mvcc_query_info->resolve_locks = true;
        query_info.mvcc_query_info->read_tso = settings.read_tso;
        RegionQueryInfo info;
        info.region_id = context.kv_context.region_id();
        info.conf_version = context.kv_context.region_epoch().conf_ver();
        info.version = context.kv_context.region_epoch().version();
        auto current_region = context.ch_context.getTMTContext().getRegionTable().getRegionById(id, info.region_id);
        if(!current_region) {
            return false;
        }
        info.range_in_table = current_region->getHandleRangeByTable(id);
        query_info.mvcc_query_info->regions_query_info.push_back(info);
        query_info.mvcc_query_info->concurrent = 0.0;
        pipeline.streams = storage->read(required_columns, query_info, context.ch_context, from_stage, max_block_size, max_streams);
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

            QuotaForIntervals & quota = context.ch_context.getQuota();

            pipeline.transform([&](auto & stream)
                               {
                                   if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
                                   {
                                       p_stream->setLimits(limits);
                                       p_stream->setQuota(quota);
                                   }
                               });
        }
        return true;
    }

    //todo return the error message
    bool InterpreterDagRequest::buildPlan(const tipb::Executor & executor, Pipeline & pipeline) {
        switch (executor.tp()) {
            case tipb::ExecType::TypeTableScan:
                return buildTSPlan(executor.tbl_scan(), pipeline);
            case tipb::ExecType::TypeIndexScan:
                // index scan is not supported
                return false;
            case tipb::ExecType::TypeSelection:
                return false;
            case tipb::ExecType::TypeAggregation:
            case tipb::ExecType::TypeStreamAgg:
                return false;
            case tipb::ExecType::TypeTopN:
                return false;
            case tipb::ExecType::TypeLimit:
                return false;
        }
    }

    BlockIO InterpreterDagRequest::execute() {
        Pipeline pipeline;
        for(const tipb::Executor & executor : dag_request.executors()) {
            if(!buildPlan(executor, pipeline)) {
                return BlockIO();
            }
        }
        // add final project
        auto stream_before_project = pipeline.firstStream();
        auto columns = stream_before_project->getHeader();
        NamesAndTypesList input_column;
        for(auto column : columns.getColumnsWithTypeAndName()) {
            input_column.emplace_back(column.name, column.type);
        }
        ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.ch_context.getSettingsRef());
        project->add(ExpressionAction::project(final_project));
        auto final_stream = std::make_shared<ExpressionBlockInputStream>(stream_before_project, project);
        BlockIO res;
        res.in = final_stream;
        return res;
    }
    InterpreterDagRequest::~InterpreterDagRequest() {

    }
}

#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/Context.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/LearnerRead.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/Transaction/Types.h>
#include <pingcap/coprocessor/Client.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <vector>

namespace DB
{
using RegionRetryList = std::list<std::reference_wrapper<const RegionInfo>>;

class TiDBTableScan
{
public:
    TiDBTableScan(const tipb::Executor * table_scan_, bool is_partition_table_scan_, const DAGContext & dag_context)
        : table_scan(table_scan_)
        , is_partition_table_scan(is_partition_table_scan_)
        , columns(is_partition_table_scan_ ? table_scan->partition_table_scan().columns() : table_scan->tbl_scan().columns())
    {
        if (is_partition_table_scan)
        {
            if (table_scan->partition_table_scan().has_table_id())
                logical_table_id = table_scan->partition_table_scan().table_id();
            else
                throw Exception("Partition table scan without table id");
            for (const auto & partition_table_id : table_scan->partition_table_scan().partition_ids())
            {
                if (dag_context.containsRegionsInfoForTable(partition_table_id))
                    physical_table_ids.push_back(partition_table_id);
            }
            std::sort(physical_table_ids.begin(), physical_table_ids.end());
        }
        else
        {
            if (table_scan->tbl_scan().has_table_id())
                logical_table_id = table_scan->tbl_scan().table_id();
            else
                throw Exception("Table scan without table id");
            physical_table_ids.push_back(logical_table_id);
        }
    }
    bool isPartitionTableScan() const
    {
        return is_partition_table_scan;
    }
    Int64 getColumnSize() const
    {
        return columns.size();
    }
    const google::protobuf::RepeatedPtrField<tipb::ColumnInfo> & getColumns() const
    {
        return columns;
    }
    void constructTableScanForRemoteRead(tipb::TableScan * tipb_table_scan, TableID table_id) const
    {
        if (is_partition_table_scan)
        {
            const auto & partition_table_scan = table_scan->partition_table_scan();
            tipb_table_scan->set_table_id(table_id);
            for (const auto & column : partition_table_scan.columns())
                *tipb_table_scan->add_columns() = column;
            tipb_table_scan->set_desc(partition_table_scan.desc());
            for (auto id : partition_table_scan.primary_column_ids())
                tipb_table_scan->add_primary_column_ids(id);
            tipb_table_scan->set_next_read_engine(tipb::EngineType::Local);
            for (auto id : partition_table_scan.primary_prefix_column_ids())
                tipb_table_scan->add_primary_prefix_column_ids(id);
        }
        else
        {
            *tipb_table_scan = table_scan->tbl_scan();
            tipb_table_scan->set_table_id(table_id);
        }
    }
    Int64 getLogicalTableID() const
    {
        return logical_table_id;
    }
    const std::vector<Int64> & getPhysicalTableIDs() const
    {
        return physical_table_ids;
    }

private:
    const tipb::Executor * table_scan;
    bool is_partition_table_scan;
    const google::protobuf::RepeatedPtrField<tipb::ColumnInfo> & columns;
    std::vector<Int64> physical_table_ids;
    Int64 logical_table_id;
};

struct RemoteRequest
{
    RemoteRequest(tipb::DAGRequest && dag_request_, DAGSchema && schema_, std::vector<pingcap::coprocessor::KeyRange> && key_ranges_)
        : dag_request(std::move(dag_request_))
        , schema(std::move(schema_))
        , key_ranges(std::move(key_ranges_))
    {}
    tipb::DAGRequest dag_request;
    DAGSchema schema;
    /// the sorted key ranges
    std::vector<pingcap::coprocessor::KeyRange> key_ranges;
};

/// DAGStorageInterpreter encapsulates operations around storage during interprete stage.
/// It's only intended to be used by DAGQueryBlockInterpreter.
/// After DAGStorageInterpreter::execute some of its members will be transfered to DAGQueryBlockInterpreter.
class DAGStorageInterpreter
{
public:
    DAGStorageInterpreter(
        Context & context_,
        const DAGQueryBlock & query_block_,
        const TiDBTableScan & table_scan,
        const std::vector<const tipb::Expr *> & conditions_,
        size_t max_streams_);

    DAGStorageInterpreter(DAGStorageInterpreter &&) = delete;
    DAGStorageInterpreter & operator=(DAGStorageInterpreter &&) = delete;

    void execute(DAGPipeline & pipeline);

    /// Members will be transfered to DAGQueryBlockInterpreter after execute

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
    std::vector<ExtraCastAfterTSMode> is_need_add_cast_column;
    /// it should be hash map because duplicated region id may occur if merge regions to retry of dag.
    RegionRetryList region_retry;
    TableLockHolders drop_locks;
    std::vector<RemoteRequest> remote_requests;
    BlockInputStreamPtr null_stream_if_empty;

private:
    struct StorageWithStructureLock
    {
        ManageableStoragePtr storage;
        TableStructureLockHolder lock;
    };
    LearnerReadSnapshot doCopLearnerRead();

    LearnerReadSnapshot doBatchCopLearnerRead();

    void doLocalRead(DAGPipeline & pipeline, size_t max_block_size);

    std::unordered_map<TableID, StorageWithStructureLock> getAndLockStorages(Int64 query_schema_version);

    std::tuple<Names, NamesAndTypes, std::vector<ExtraCastAfterTSMode>, String> getColumnsForTableScan(Int64 max_columns_to_read);

    void buildRemoteRequest();

    void releaseAlterLocks();

    std::unordered_map<TableID, SelectQueryInfo> generateSelectQueryInfos();

    /// passed from caller, doesn't change during DAGStorageInterpreter's lifetime

    Context & context;
    const DAGQueryBlock & query_block;
    const TiDBTableScan & table_scan;
    const std::vector<const tipb::Expr *> & conditions;
    size_t max_streams;
    LogWithPrefixPtr log;

    /// derived from other members, doesn't change during DAGStorageInterpreter's lifetime

    TableID logical_table_id;
    const Settings & settings;
    TMTContext & tmt;

    /// Intermediate variables shared by multiple member functions

    std::unique_ptr<MvccQueryInfo> mvcc_query_info;
    // We need to validate regions snapshot after getting streams from storage.
    LearnerReadSnapshot learner_read_snapshot;
    /// Table from where to read data, if not subquery.
    /// Hold read lock on both `alter_lock` and `drop_lock` until the local input streams are created.
    /// We need an immutable structure to build the TableScan operator and create snapshot input streams
    /// of storage. After the input streams created, the `alter_lock` can be released so that reading
    /// won't block DDL operations.
    std::unordered_map<TableID, StorageWithStructureLock> storages_with_structure_lock;
    ManageableStoragePtr storage_for_logical_table;
    Names required_columns;
    NamesAndTypes source_columns;
    String handle_column_name;
};

} // namespace DB

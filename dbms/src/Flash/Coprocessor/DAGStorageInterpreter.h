#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/Context.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/LearnerRead.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TMTStorages.h>
#include <Storages/Transaction/Types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <vector>

namespace DB
{
using RegionRetryList = std::list<std::reference_wrapper<const RegionInfo>>;

/// DAGStorageInterpreter encapsulates operations around storage during interprete stage.
/// It's only intended to be used by DAGQueryBlockInterpreter.
/// After DAGStorageInterpreter::execute some of its members will be transfered to DAGQueryBlockInterpreter.
class DAGStorageInterpreter
{
public:
    DAGStorageInterpreter(
        Context & context_,
        const DAGQuerySource & dag_,
        const DAGQueryBlock & query_block_,
        const tipb::TableScan & ts,
        const std::vector<const tipb::Expr *> & conditions_,
        size_t max_streams_,
        Poco::Logger * log_,
        Poco::Logger * mpp_task_log_ = nullptr);

    DAGStorageInterpreter(DAGStorageInterpreter &&) = delete;
    DAGStorageInterpreter & operator=(DAGStorageInterpreter &&) = delete;

    void execute(DAGPipeline & pipeline);

    /// Members will be transfered to DAGQueryBlockInterpreter after execute

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
    BoolVec is_timestamp_column;
    /// it should be hash map because duplicated region id may occur if merge regions to retry of dag.
    RegionRetryList region_retry;
    /// Hold read lock on both `alter_lock` and `drop_lock` until the local input streams are created.
    /// We need an immuntable structure to build the TableScan operator and create snapshot input streams
    /// of storage. After the input streams created, the `alter_lock` can be released so that reading
    /// won't block DDL operations.
    TableStructureLockHolder table_structure_lock;
    std::optional<tipb::DAGRequest> dag_request;
    std::optional<DAGSchema> dag_schema;
    BlockInputStreamPtr null_stream_if_empty;
private:
    LearnerReadSnapshot doCopLearnerRead();

    LearnerReadSnapshot doBatchCopLearnerRead();

    void doLocalRead(DAGPipeline & pipeline, size_t max_block_size);

    std::tuple<ManageableStoragePtr, TableStructureLockHolder> getAndLockStorage(Int64 query_schema_version);

    std::tuple<Names, NamesAndTypes, BoolVec, String> getColumnsForTableScan(Int64 max_columns_to_read);

    std::tuple<std::optional<tipb::DAGRequest>, std::optional<DAGSchema>> buildRemoteTS();

    /// passed from caller, doesn't change during DAGStorageInterpreter's lifetime

    Context & context;
    const DAGQuerySource & dag;
    const DAGQueryBlock & query_block;
    const tipb::TableScan & table_scan;
    const std::vector<const tipb::Expr *> & conditions;
    size_t max_streams;
    Poco::Logger * log;
    Poco::Logger * mpp_task_log;

    /// derived from other members, doesn't change during DAGStorageInterpreter's lifetime

    TableID table_id;
    const Settings & settings;
    TMTContext & tmt;

    /// Intermediate variables shared by multiple member functions

    std::unique_ptr<MvccQueryInfo> mvcc_query_info;
    // We need to validate regions snapshot after getting streams from storage.
    LearnerReadSnapshot learner_read_snapshot;
    /// Table from where to read data, if not subquery.
    ManageableStoragePtr storage;
    Names required_columns;
    NamesAndTypes source_columns;
    String handle_column_name;
};

} // namespace DB


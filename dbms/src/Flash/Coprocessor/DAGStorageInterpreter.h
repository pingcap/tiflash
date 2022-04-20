// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Coprocessor/TiDBStorageTable.h>
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
using TablesRegionInfoMap = std::unordered_map<Int64, std::reference_wrapper<const RegionInfoMap>>;
/// DAGStorageInterpreter encapsulates operations around storage during interprete stage.
/// It's only intended to be used by DAGQueryBlockInterpreter.
/// After DAGStorageInterpreter::execute some of its members will be transfered to DAGQueryBlockInterpreter.
class DAGStorageInterpreter
{
public:
    DAGStorageInterpreter(
        Context & context_,
        const TiDBStorageTable & storage_table_,
        const String & pushed_down_filter_id_,
        const std::vector<const tipb::Expr *> & pushed_down_conditions_,
        size_t max_streams_);

    DAGStorageInterpreter(DAGStorageInterpreter &&) = delete;
    DAGStorageInterpreter & operator=(DAGStorageInterpreter &&) = delete;

    void execute(DAGPipeline & pipeline);

    /// Members will be transfered to DAGQueryBlockInterpreter after execute

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
    std::vector<ExtraCastAfterTSMode> is_need_add_cast_column;
    /// it shouldn't be hash map because duplicated region id may occur if merge regions to retry of dag.
    RegionRetryList region_retry_from_local_region;
    std::vector<RemoteRequest> remote_requests;
    BlockInputStreamPtr null_stream_if_empty;

private:
    LearnerReadSnapshot doCopLearnerRead();

    LearnerReadSnapshot doBatchCopLearnerRead();

    void doLocalRead(DAGPipeline & pipeline, size_t max_block_size);

    void buildRemoteRequests();

    std::unordered_map<TableID, SelectQueryInfo> generateSelectQueryInfos();

    /// passed from caller, doesn't change during DAGStorageInterpreter's lifetime

    Context & context;
    const TiDBStorageTable & storage_table;
    const String & pushed_down_filter_id;
    const std::vector<const tipb::Expr *> & pushed_down_conditions;
    size_t max_streams;
    LoggerPtr log;

    /// derived from other members, doesn't change during DAGStorageInterpreter's lifetime

    TableID logical_table_id;
    const Settings & settings;
    TMTContext & tmt;

    /// Intermediate variables shared by multiple member functions

    std::unique_ptr<MvccQueryInfo> mvcc_query_info;
    // We need to validate regions snapshot after getting streams from storage.
    LearnerReadSnapshot learner_read_snapshot;
    NamesAndTypes source_columns;
};

} // namespace DB

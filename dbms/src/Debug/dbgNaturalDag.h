// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Coprocessor/RegionInfo.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/MultiRaft/RegionMeta.h>
#include <TiDB/Schema/TiDB.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/mpp.pb.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

namespace DB
{
class Context;
class TiDBSchemaSyncerManager;

/// NaturalDag accepts a dag request json file produced from TiDB, and provide following functionalities:
/// 1. Parse json file to load tables, regions, dag request, dag response information
/// 2. Build regions, tables, databases in TiFlash using Mock Utils(Drop tables, databases if exists before reconstructions)
/// 3. Drop related tables, Databases
class NaturalDag
{
public:
    using ReqRspVec = std::vector<std::pair<coprocessor::Request, coprocessor::Response>>;
    using BatchReqRspVec = std::vector<std::pair<coprocessor::BatchRequest, coprocessor::BatchResponse>>;
    /// DispatchTaskResponse only contains status info, without data which are we care about, so use MPPDataPacket instead
    using MPPReqRspVec = std::vector<std::pair<mpp::DispatchTaskRequest, mpp::MPPDataPacket>>;
    NaturalDag(const String & json_dag_path_, Poco::Logger * log_)
        : json_dag_path(json_dag_path_)
        , log(log_) //To keep the same as MockTests
    {}

    void init();
    void build(Context & context);
    const ReqRspVec & getReqAndRspVec() const { return req_rsp; }
    const BatchReqRspVec & getBatchReqAndRspVec() const { return batch_req_rsp; }
    const MPPReqRspVec & getMPPReqAndRspVec() const { return mpp_req_rsp; }
    const std::vector<int32_t> & getReqIDVec() const { return req_id_vec; }
    bool continueWhenError() const { return continue_when_error; }
    static void clean(Context & context);

private:
    struct LoadedRegionInfo
    {
        uint64_t id;
        uint64_t version;
        uint64_t conf_ver;
        TiKVKey start;
        TiKVKey end;
        std::vector<TiKVKeyValue> pairs;
    };

    struct LoadedTableData
    {
        uint64_t id;
        TiDB::TableInfo meta;
        std::vector<LoadedRegionInfo> regions;
    };

    using LoadedTableMap = std::unordered_map<TableID, LoadedTableData>;
    using TableIDVec = std::vector<TableID>;
    using JSONObjectPtr = Poco::JSON::Object::Ptr;

    void loadTables(const JSONObjectPtr & obj);
    LoadedRegionInfo loadRegion(const Poco::Dynamic::Var & region_json) const;
    void loadReqAndRsp(const JSONObjectPtr & obj);
    static void buildDatabase(
        Context & context,
        std::shared_ptr<TiDBSchemaSyncerManager> & schema_syncer,
        const String & db_name);
    void buildTables(Context & context);
    static const String & getDatabaseName();

    String json_dag_path;
    Poco::Logger * log;
    LoadedTableMap tables;
    TableIDVec table_ids;
    bool continue_when_error = false;
    std::vector<int32_t> req_id_vec;
    ReqRspVec req_rsp;
    BatchReqRspVec batch_req_rsp;
    MPPReqRspVec mpp_req_rsp;
};
} // namespace DB

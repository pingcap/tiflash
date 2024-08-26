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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Parsers/IAST_fwd.h>
#include <TiDB/Schema/TiDB_fwd.h>
#include <kvproto/mpp.pb.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NO_SUCH_COLUMN_IN_TABLE;
} // namespace ErrorCodes

class ASTFunction;
class ASTIdentifier;
class Context;

struct MPPCtx
{
    Timestamp start_ts;
    Int64 next_task_id;
    std::vector<Int64> sender_target_task_ids;
    explicit MPPCtx(Timestamp start_ts_)
        : start_ts(start_ts_)
        , next_task_id(1)
    {}
};

using MPPCtxPtr = std::shared_ptr<MPPCtx>;

struct MPPInfo
{
    Timestamp start_ts;
    Int64 gather_id;
    UInt64 query_ts;
    UInt64 server_id;
    UInt64 local_query_id;
    Int64 partition_id;
    Int64 task_id;
    const std::vector<Int64> sender_target_task_ids;
    const std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;

    MPPInfo(
        Timestamp start_ts_,
        Int64 gather_id_,
        UInt64 query_ts_,
        UInt64 server_id_,
        UInt64 local_query_id_,
        Int64 partition_id_,
        Int64 task_id_,
        const std::vector<Int64> & sender_target_task_ids_,
        const std::unordered_map<String, std::vector<Int64>> & receiver_source_task_ids_map_)
        : start_ts(start_ts_)
        , gather_id(gather_id_)
        , query_ts(query_ts_)
        , server_id(server_id_)
        , local_query_id(local_query_id_)
        , partition_id(partition_id_)
        , task_id(task_id_)
        , sender_target_task_ids(sender_target_task_ids_)
        , receiver_source_task_ids_map(receiver_source_task_ids_map_)
    {}
};

struct TaskMeta
{
    UInt64 start_ts = 0;
    Int64 task_id = 0;
    Int64 partition_id = 0;
};

using TaskMetas = std::vector<TaskMeta>;
void literalFieldToTiPBExpr(const TiDB::ColumnInfo & ci, const Field & field, tipb::Expr * expr, Int32 collator_id);
void literalToPB(tipb::Expr * expr, const Field & value, int32_t collator_id);
String getFunctionNameForConstantFolding(tipb::Expr * expr);
void foldConstant(tipb::Expr * expr, int32_t collator_id, const Context & context);
void functionToPB(
    const DAGSchema & input,
    ASTFunction * func,
    tipb::Expr * expr,
    int32_t collator_id,
    const Context & context);
void identifierToPB(const DAGSchema & input, ASTIdentifier * id, tipb::Expr * expr, int32_t collator_id);
void astToPB(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, int32_t collator_id, const Context & context);
void collectUsedColumnsFromExpr(const DAGSchema & input, ASTPtr ast, std::unordered_set<String> & used_columns);
TiDB::ColumnInfo compileExpr(const DAGSchema & input, ASTPtr ast);
void compileFilter(const DAGSchema & input, ASTPtr ast, ASTs & conditions);
void fillTaskMetaWithMPPInfo(mpp::TaskMeta & task_meta, const MPPInfo & mpp_info);

} // namespace DB

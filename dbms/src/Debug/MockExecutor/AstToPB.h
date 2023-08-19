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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/FieldToDataType.h>
#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/FuncSigMap.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NO_SUCH_COLUMN_IN_TABLE;
} // namespace ErrorCodes
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
    Int64 partition_id;
    Int64 task_id;
    const std::vector<Int64> sender_target_task_ids;
    const std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;

    MPPInfo(
        Timestamp start_ts_,
        Int64 partition_id_,
        Int64 task_id_,
        const std::vector<Int64> & sender_target_task_ids_,
        const std::unordered_map<String, std::vector<Int64>> & receiver_source_task_ids_map_)
        : start_ts(start_ts_)
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
void literalFieldToTiPBExpr(const ColumnInfo & ci, const Field & field, tipb::Expr * expr, Int32 collator_id);
void literalToPB(tipb::Expr * expr, const Field & value, int32_t collator_id);
String getFunctionNameForConstantFolding(tipb::Expr * expr);
void foldConstant(tipb::Expr * expr, int32_t collator_id, const Context & context);
void functionToPB(const DAGSchema & input, ASTFunction * func, tipb::Expr * expr, int32_t collator_id, const Context & context);
void identifierToPB(const DAGSchema & input, ASTIdentifier * id, tipb::Expr * expr, int32_t collator_id);
void astToPB(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, int32_t collator_id, const Context & context);
void collectUsedColumnsFromExpr(const DAGSchema & input, ASTPtr ast, std::unordered_set<String> & used_columns);
TiDB::ColumnInfo compileExpr(const DAGSchema & input, ASTPtr ast);
void compileFilter(const DAGSchema & input, ASTPtr ast, std::vector<ASTPtr> & conditions);

} // namespace DB

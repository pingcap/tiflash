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

#include <Interpreters/Set.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/KVStore/Types.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{
class ASTSelectQuery;

struct DAGQueryInfo;

namespace DM
{

class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

class FilterParser
{
public:
    /// From dag.
    using AttrCreatorByColumnID = std::function<Attr(const DB::ColumnID)>;
    static RSOperatorPtr parseDAGQuery(
        const DAGQueryInfo & dag_info,
        const ColumnDefines & columns_to_read,
        AttrCreatorByColumnID && creator,
        const LoggerPtr & log);

    // only for runtime filter in predicate
    static RSOperatorPtr parseRFInExpr(
        tipb::RuntimeFilterType rf_type,
        const tipb::Expr & target_expr,
        const ColumnDefines & columns_to_read,
        const std::set<Field> & setElements,
        const TimezoneInfo & timezone_info);

    static bool isRSFilterSupportType(Int32 field_type);

    /// Some helper structure

    enum RSFilterType
    {
        Unsupported = 0,

        // logical
        Not = 1,
        Or,
        And,
        // compare
        Equal,
        NotEqual,
        Greater,
        GreaterEqual,
        Less,
        LessEqual,

        In,
        NotIn,

        Like,
        NotLike,

        IsNull,
    };

    static std::unordered_map<tipb::ScalarFuncSig, RSFilterType> scalar_func_rs_filter_map;
};

} // namespace DM
} // namespace DB

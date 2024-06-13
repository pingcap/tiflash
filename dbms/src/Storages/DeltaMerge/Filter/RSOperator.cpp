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

#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Filter/And.h>
#include <Storages/DeltaMerge/Filter/Equal.h>
#include <Storages/DeltaMerge/Filter/Greater.h>
#include <Storages/DeltaMerge/Filter/GreaterEqual.h>
#include <Storages/DeltaMerge/Filter/In.h>
#include <Storages/DeltaMerge/Filter/IsNull.h>
#include <Storages/DeltaMerge/Filter/Less.h>
#include <Storages/DeltaMerge/Filter/LessEqual.h>
#include <Storages/DeltaMerge/Filter/Like.h>
#include <Storages/DeltaMerge/Filter/Not.h>
#include <Storages/DeltaMerge/Filter/NotEqual.h>
#include <Storages/DeltaMerge/Filter/Or.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Filter/Unsupported.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>

namespace DB::DM
{

// clang-format off
RSOperatorPtr createAnd(const RSOperators & children)                           { return std::make_shared<And>(children); }
RSOperatorPtr createEqual(const Attr & attr, const Field & value)               { return std::make_shared<Equal>(attr, value); }
RSOperatorPtr createGreater(const Attr & attr, const Field & value)             { return std::make_shared<Greater>(attr, value); }
RSOperatorPtr createGreaterEqual(const Attr & attr, const Field & value)        { return std::make_shared<GreaterEqual>(attr, value); }
RSOperatorPtr createIn(const Attr & attr, const Fields & values)                { return std::make_shared<In>(attr, values); }
RSOperatorPtr createLess(const Attr & attr, const Field & value)                { return std::make_shared<Less>(attr, value); }
RSOperatorPtr createLessEqual(const Attr & attr, const Field & value)           { return std::make_shared<LessEqual>(attr, value); }
RSOperatorPtr createLike(const Attr & attr, const Field & value)                { return std::make_shared<Like>(attr, value); }
RSOperatorPtr createNot(const RSOperatorPtr & op)                               { return std::make_shared<Not>(op); }
RSOperatorPtr createNotEqual(const Attr & attr, const Field & value)            { return std::make_shared<NotEqual>(attr, value); }
RSOperatorPtr createOr(const RSOperators & children)                            { return std::make_shared<Or>(children); }
RSOperatorPtr createIsNull(const Attr & attr)                                   { return std::make_shared<IsNull>(attr);}
RSOperatorPtr createUnsupported(const String & content, const String & reason)  { return std::make_shared<Unsupported>(content, reason); }
// clang-format on

RSOperatorPtr RSOperator::build(
    const std::unique_ptr<DAGQueryInfo> & dag_query,
    const ColumnDefines & columns_to_read,
    const ColumnDefines & table_column_defines,
    bool enable_rs_filter,
    const LoggerPtr & tracing_logger)
{
    RUNTIME_CHECK(dag_query != nullptr);
    // build rough set operator
    if (unlikely(!enable_rs_filter))
    {
        LOG_DEBUG(tracing_logger, "Rough set filter is disabled.");
        return EMPTY_RS_OPERATOR;
    }

    /// Query from TiDB / TiSpark
    auto create_attr_by_column_id = [&table_column_defines](ColumnID column_id) -> Attr {
        auto iter = std::find_if(
            table_column_defines.begin(),
            table_column_defines.end(),
            [column_id](const ColumnDefine & d) -> bool { return d.id == column_id; });
        if (iter != table_column_defines.end())
            return Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
        return Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
    };
    DM::RSOperatorPtr rs_operator
        = FilterParser::parseDAGQuery(*dag_query, columns_to_read, std::move(create_attr_by_column_id), tracing_logger);
    if (likely(rs_operator != DM::EMPTY_RS_OPERATOR))
        LOG_DEBUG(tracing_logger, "Rough set filter: {}", rs_operator->toDebugString());

    return rs_operator;
}

} // namespace DB::DM

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
#include <TiDB/Schema/TiDB.h>


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
RSOperatorPtr createUnsupported(const String & reason)                          { return std::make_shared<Unsupported>(reason); }
// clang-format on

RSOperatorPtr RSOperator::build(
    const std::unique_ptr<DAGQueryInfo> & dag_query,
    const TiDB::ColumnInfos & scan_column_infos,
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

    // Query from TiDB / TiSpark
    FilterParser::ColumnIDToAttrMap column_id_to_attr;
    for (const auto & col_info : scan_column_infos)
    {
        auto iter = std::find_if(
            table_column_defines.cbegin(),
            table_column_defines.cend(),
            [col_id = col_info.id](const ColumnDefine & cd) { return cd.id == col_id; });
        if (iter == table_column_defines.cend())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column id {} not found in table column defines", col_info.id);
        const auto & cd = *iter;
        column_id_to_attr[cd.id] = Attr{.col_name = cd.name, .col_id = cd.id, .type = cd.type};
    }

    auto rs_operator = FilterParser::parseDAGQuery(*dag_query, scan_column_infos, column_id_to_attr, tracing_logger);
    if (likely(rs_operator != DM::EMPTY_RS_OPERATOR))
        LOG_DEBUG(tracing_logger, "Rough set filter: {}", rs_operator->toDebugString());

    return rs_operator;
}

} // namespace DB::DM

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

#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{
struct SelectQueryInfo;
}

namespace DB::DM
{

struct PushDownFilter;
using PushDownFilterPtr = std::shared_ptr<PushDownFilter>;
inline static const PushDownFilterPtr EMPTY_FILTER{};

class QueryFilter;
using QueryFilterPtr = std::shared_ptr<QueryFilter>;

class QueryFilter
{
public:
    QueryFilter(
        String && filter_name_,
        const ExpressionActionsPtr & beofre_where_,
        const ExpressionActionsPtr & project_after_where_,
        const ColumnDefinesPtr & filter_columns_,
        const String filter_column_name_,
        const ExpressionActionsPtr & extra_cast_,
        const ColumnDefinesPtr & columns_after_cast_)
        : filter_name(std::move(filter_name_))
        , before_where(beofre_where_)
        , project_after_where(project_after_where_)
        , filter_column_name(std::move(filter_column_name_))
        , filter_columns(filter_columns_)
        , extra_cast(extra_cast_)
        , columns_after_cast(columns_after_cast_)
    {}

    const String & name() const { return filter_name; }
    bool empty() const { return before_where == nullptr; }
    BlockInputStreamPtr buildFilterInputStream(BlockInputStreamPtr stream, bool need_project, const String & tracing_id)
        const;

    static QueryFilterPtr build(
        String && filter_name,
        const ColumnInfos & table_scan_column_info,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
        const ColumnDefines & columns_to_read,
        const Context & context,
        const LoggerPtr & tracing_logger);

    String filter_name;
    // Filter expression actions and the name of the tmp filter column
    // Used construct the FilterBlockInputStream
    const ExpressionActionsPtr before_where;
    // The projection after the filter, used to remove the tmp filter column
    // Used to construct the ExpressionBlockInputStream
    // Note: ususally we will remove the tmp filter column in the LateMaterializationBlockInputStream, this only used for unexpected cases
    const ExpressionActionsPtr project_after_where;
    const String filter_column_name;
    // The columns needed by the filter expression
    const ColumnDefinesPtr filter_columns;
    // The expression actions used to cast the timestamp/datetime column
    const ExpressionActionsPtr extra_cast;
    // If the extra_cast is not null, the types of the columns may be changed
    const ColumnDefinesPtr columns_after_cast;
};

struct PushDownFilter
{
public:
    PushDownFilter(
        const RSOperatorPtr & rs_operator_,
        const QueryFilterPtr & lm_filter_,
        const QueryFilterPtr & rest_filter_)
        : rs_operator(rs_operator_)
        , lm_filter(lm_filter_)
        , rest_filter(rest_filter_)
    {}

    bool hasLMFilter() const { return lm_filter && !lm_filter->empty(); }
    bool hasRestFilter() const { return rest_filter && !rest_filter->empty(); }
    bool empty() const { return !hasLMFilter() && !hasRestFilter(); }
    ColumnDefinesPtr columnsAfterCast() const
    {
        if (lm_filter && lm_filter->extra_cast)
        {
            return lm_filter->columns_after_cast;
        }
        if (rest_filter && rest_filter->extra_cast)
        {
            return rest_filter->columns_after_cast;
        }
        return nullptr;
    }

    // Use by StorageDisaggregated.
    static PushDownFilterPtr build(
        const DM::RSOperatorPtr & rs_operator,
        const ColumnInfos & table_scan_column_info,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
        const ColumnDefines & columns_to_read,
        const Context & context,
        const LoggerPtr & tracing_logger);

    // Use by StorageDeltaMerge.
    static PushDownFilterPtr build(
        const SelectQueryInfo & query_info,
        const ColumnDefines & columns_to_read,
        const ColumnDefines & table_column_defines,
        const Context & context,
        const LoggerPtr & tracing_logger);

    static PushDownFilterPtr build(const RSOperatorPtr & rs_operator)
    {
        return std::make_shared<PushDownFilter>(rs_operator, nullptr, nullptr);
    }

    RSOperatorPtr rs_operator;
    QueryFilterPtr lm_filter;
    QueryFilterPtr rest_filter;
};

} // namespace DB::DM

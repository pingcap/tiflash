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

class PushDownExecutor;
using PushDownExecutorPtr = std::shared_ptr<PushDownExecutor>;
inline static const PushDownExecutorPtr EMPTY_FILTER{};

class PushDownExecutor
{
public:
    PushDownExecutor(
        const RSOperatorPtr & rs_operator_,
        const ANNQueryInfoPtr & ann_query_info_,
        const ExpressionActionsPtr & beofre_where_,
        const ExpressionActionsPtr & project_after_where_,
        const ColumnDefinesPtr & filter_columns_,
        const String filter_column_name_,
        const ExpressionActionsPtr & extra_cast_,
        const ColumnDefinesPtr & columns_after_cast_)
        : rs_operator(rs_operator_)
        , before_where(beofre_where_)
        , project_after_where(project_after_where_)
        , filter_column_name(std::move(filter_column_name_))
        , filter_columns(filter_columns_)
        , extra_cast(extra_cast_)
        , columns_after_cast(columns_after_cast_)
        , ann_query_info(ann_query_info_)
    {}

    explicit PushDownExecutor(const RSOperatorPtr & rs_operator_, const ANNQueryInfoPtr & ann_query_info_ = nullptr)
        : rs_operator(rs_operator_)
        , ann_query_info(ann_query_info_)
    {}

    explicit PushDownExecutor(const ANNQueryInfoPtr & ann_query_info_)
        : ann_query_info(ann_query_info_)
    {}

    // Use by StorageDisaggregated.
    static PushDownExecutorPtr build(
        const DM::RSOperatorPtr & rs_operator,
        const ANNQueryInfoPtr & ann_query_info,
        const TiDB::ColumnInfos & table_scan_column_info,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters,
        const ColumnDefines & columns_to_read,
        const Context & context,
        const LoggerPtr & tracing_logger);

    // Use by StorageDeltaMerge.
    static DM::PushDownExecutorPtr build(
        const SelectQueryInfo & query_info,
        const ColumnDefines & columns_to_read,
        const ColumnDefines & table_column_defines,
        const Context & context,
        const LoggerPtr & tracing_logger);

    // Rough set operator
    RSOperatorPtr rs_operator;
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
    // The ANNQueryInfo contains the information of the ANN index
    const ANNQueryInfoPtr ann_query_info;
};

} // namespace DB::DM

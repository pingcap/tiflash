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

#include <DataStreams/FilterTransformAction.h>
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

class PredicateFilter;
using PredicateFilterPtr = std::shared_ptr<PredicateFilter>;

class PredicateFilter
{
public:
    PredicateFilter(
        const ExpressionActionsPtr & before_where_,
        const ExpressionActionsPtr & project_after_where_,
        const String filter_column_name_,
        const ExpressionActionsPtr & extra_cast_,
        const LoggerPtr & log_)
        : before_where(before_where_)
        , project_after_where(project_after_where_)
        , filter_column_name(std::move(filter_column_name_))
        , extra_cast(extra_cast_)
        , log(log_)
    {}

    bool empty() const { return before_where == nullptr; }

    BlockInputStreamPtr buildFilterInputStream( //
        BlockInputStreamPtr stream,
        bool need_project) const;

    FilterTransformAction getFilterTransformAction(const Block & header) const
    {
        return FilterTransformAction{header, before_where, filter_column_name};
    }

    static bool transformBlock(
        ExpressionActionsPtr & extra_cast,
        FilterTransformAction & filter_trans,
        ExpressionActions & project,
        Block & block,
        IColumn::Filter & filter_result,
        bool return_filter)
    {
        if (extra_cast)
        {
            extra_cast->execute(block);
        }

        FilterPtr f = nullptr;
        if (filter_trans.transform(block, f, return_filter))
        {
            // `f` points to a filter column in block.
            // This column will be destroy after projecting, so copy/swap it.
            if (return_filter)
            {
                if (f)
                    filter_result.swap(*f); // Some
                else
                    filter_result.resize(0); // All
            }
            project.execute(block);
            return true; // Some or All, according to the content of filter_result
        }
        return false; // None
    }

    static std::pair<PredicateFilterPtr, std::unordered_map<ColumnID, DataTypePtr>> build(
        const ColumnDefines & filter_columns,
        const ColumnDefines & input_columns,
        const ColumnInfos & table_scan_column_infos,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        const ColumnDefines & table_scan_columns_to_read,
        const Context & context,
        const LoggerPtr & log);

    // Filter expression actions and the name of the tmp filter column
    // Used construct the FilterBlockInputStream
    const ExpressionActionsPtr before_where;
    // The projection after the filter, used to remove the tmp filter column
    // Used to construct the ExpressionBlockInputStream
    // Note: ususally we will remove the tmp filter column in the LateMaterializationBlockInputStream, this only used for unexpected cases
    const ExpressionActionsPtr project_after_where;
    const String filter_column_name;
    // The expression actions used to cast the timestamp/datetime column
    const ExpressionActionsPtr extra_cast;

    LoggerPtr log;
};

struct PushDownFilter
{
public:
    PushDownFilter(
        const RSOperatorPtr & rs_operator_,
        const PredicateFilterPtr & lm_filter_,
        const PredicateFilterPtr & rest_filter_,
        const ColumnDefinesPtr & lm_columns_,
        const ColumnDefinesPtr & rest_columns_,
        const ColumnDefinesPtr & casted_columns_)
        : rs_operator(rs_operator_)
        , lm_filter(lm_filter_)
        , rest_filter(rest_filter_)
        , lm_columns(lm_columns_)
        , rest_columns(rest_columns_)
        , casted_columns(casted_columns_)
    {}

    bool hasLMFilter() const { return lm_filter && !lm_filter->empty(); }
    bool hasRestFilter() const { return rest_filter && !rest_filter->empty(); }
    bool empty() const { return !hasLMFilter() && !hasRestFilter(); }
    const ColumnDefinesPtr & castedColumns() const { return casted_columns; }
    const ColumnDefinesPtr & LMColumns() const { return lm_columns; }
    const ColumnDefinesPtr & restColumns() const { return rest_columns; }

    // Use by StorageDisaggregated.
    static PushDownFilterPtr build(
        const RSOperatorPtr & rs_operator,
        const ColumnInfos & table_scan_column_infos,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & lm_filter_exprs,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & rest_filter_exprs,
        const ColumnDefines & table_scan_columns_to_read,
        const Context & context,
        const bool keep_order,
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
        return std::make_shared<PushDownFilter>(rs_operator, nullptr, nullptr, nullptr, nullptr, nullptr);
    }

    RSOperatorPtr rs_operator;
    PredicateFilterPtr lm_filter;
    PredicateFilterPtr rest_filter;

private:
    ColumnDefinesPtr lm_columns;
    ColumnDefinesPtr rest_columns;
    ColumnDefinesPtr casted_columns;
};

} // namespace DB::DM

template <>
struct fmt::formatter<DB::DataTypePtr>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DataTypePtr & t, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", t->getName());
    }
};

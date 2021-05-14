#pragma once

#include <unordered_map>

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQuerySource.h>

namespace DB
{
// DAGQueryInfo contains filter information in dag request, it will
// be used to extracted key conditions by storage engine
struct DAGQueryInfo
{
    DAGQueryInfo(const std::vector<const tipb::Expr *> & filters_, DAGPreparedSets dag_sets_,
        const std::vector<NameAndTypePair> & source_columns_, const TimezoneInfo & timezone_info_)
        : filters(filters_), dag_sets(std::move(dag_sets_)), source_columns(source_columns_), timezone_info(timezone_info_){};
    // filters in dag request
    const std::vector<const tipb::Expr *> & filters;
    // Prepared sets extracted from dag request, which are used for indices
    // by storage engine.
    DAGPreparedSets dag_sets;
    const std::vector<NameAndTypePair> & source_columns;

    const TimezoneInfo & timezone_info;
};
} // namespace DB

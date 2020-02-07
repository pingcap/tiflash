#pragma once

#include <unordered_map>

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQuerySource.h>

namespace DB
{

struct DAGQueryInfo
{
    DAGQueryInfo(const std::vector<const tipb::Expr *> & conditions_, DAGPreparedSets dag_sets_, const std::vector<NameAndTypePair> & source_columns_)
        : conditions(conditions_), dag_sets(std::move(dag_sets_)), source_columns(source_columns_){};
    const std::vector<const tipb::Expr *> & conditions;
    DAGPreparedSets dag_sets;
    const std::vector<NameAndTypePair> & source_columns;
};
} // namespace DB

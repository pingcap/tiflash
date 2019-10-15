#pragma once

#include <unordered_map>

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQuerySource.h>

namespace DB
{

struct DAGQueryInfo
{
    DAGQueryInfo(const DAGQuerySource & dag_, DAGPreparedSets dag_sets_, std::vector<NameAndTypePair> & source_columns_)
        : dag(dag_), dag_sets(std::move(dag_sets_)), source_columns(source_columns_){};
    const DAGQuerySource & dag;
    DAGPreparedSets dag_sets;
    const std::vector<NameAndTypePair> & source_columns;
};
} // namespace DB

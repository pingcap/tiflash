#pragma once

#include <unordered_map>

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQuerySource.h>

namespace DB
{

struct DAGQueryInfo
{
    DAGQueryInfo(const DAGQuerySource & dag_, DAGPreparedSets dag_sets_, std::vector<NameAndTypePair> & source_columns_)
        : dag(dag_), dag_sets(std::move(dag_sets_))
    {
        for (auto & c : source_columns_)
            source_columns.emplace_back(c.name, c.type);
    };
    const DAGQuerySource & dag;
    DAGPreparedSets dag_sets;
    NamesAndTypesList source_columns;
};
} // namespace DB

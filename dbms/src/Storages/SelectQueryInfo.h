#pragma once

#include <memory>
#include <unordered_map>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class Set;
using SetPtr = std::shared_ptr<Set>;

/// Information about calculated sets in right hand side of IN.
using PreparedSets = std::unordered_map<IAST *, SetPtr>;

struct MvccQueryInfo;
struct DAGQueryInfo;

/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;

    std::unique_ptr<MvccQueryInfo> mvcc_query_info;

    std::unique_ptr<DAGQueryInfo> dag_query;

    SelectQueryInfo();

    SelectQueryInfo(const SelectQueryInfo & query_info_);

    SelectQueryInfo(SelectQueryInfo && query_info_);

    ~SelectQueryInfo();

    bool fromAST() const { return dag_query == nullptr; };
};

} // namespace DB

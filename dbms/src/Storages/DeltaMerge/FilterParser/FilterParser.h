#pragma once

#include <cassert>
#include <sstream>

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <tipb/expression.pb.h>

namespace DB
{

class ASTSelectQuery;

struct DAGQueryInfo;

namespace DM
{

class FilterParser
{
public:
    /// From ast.
    using AttrCreatorByColumnName = std::function<Attr(const String &)>;
    static RSOperatorPtr parseSelectQuery(const ASTSelectQuery & query, AttrCreatorByColumnName && creator, Poco::Logger * log);

public:
    /// From dag.
    using AttrCreatorByColumnID = std::function<Attr(const ColumnID)>;
    static RSOperatorPtr parseDAGQuery(const DAGQueryInfo & dag_info, AttrCreatorByColumnID && creator, Poco::Logger * log);

    /// Some helper structur

    enum RSFilterType
    {
        // logical
        Not = 0,
        Or,
        And,
        // compare
        Equal,
        NotEqual,
        Greater,
        GreaterEqual,
        Less,
        LessEuqal,

        In,
        NotIn,

        Like,
        NotLike,

        Unsupported = 254,
    };

    static std::unordered_map<tipb::ScalarFuncSig, RSFilterType> scalar_func_rs_filter_map;
};

} // namespace DM
} // namespace DB

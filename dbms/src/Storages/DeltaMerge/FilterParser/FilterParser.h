#pragma once

#include <cassert>
#include <sstream>
#include <tipb/expression.pb.h>

namespace DB
{

class ASTSelectQuery;

struct DAGQueryInfo;

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

class FilterParser
{
public:
    /// From ast.
    using AttrCreatorByColumnName = std::function<Attr(const String &)>;
    static RSOperatorPtr parseSelectQuery(const ASTSelectQuery & query, AttrCreatorByColumnName && creator, Poco::Logger * log);

public:
    /// From dag.
    using AttrCreatorByColumnID = std::function<Attr(const ColumnID)>;
    static RSOperatorPtr parseDAGQuery(const DAGQueryInfo &     dag_info,
                                       const ColumnDefines &    columns_to_read,
                                       AttrCreatorByColumnID && creator,
                                       Poco::Logger *           log);

    /// Some helper structure

    enum RSFilterType
    {
        Unsupported = 0,

        // logical
        Not = 1,
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
    };

    static std::unordered_map<tipb::ScalarFuncSig, RSFilterType> scalar_func_rs_filter_map;
};

} // namespace DM
} // namespace DB

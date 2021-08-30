#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/Types.h>
#include <tipb/expression.pb.h>

#include <functional>
#include <memory>
#include <unordered_map>

namespace Poco
{
class Logger;
}

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
    /// From dag.
    using AttrCreatorByColumnID = std::function<Attr(const ColumnID)>;
    static RSOperatorPtr parseDAGQuery(
        const DAGQueryInfo & dag_info,
        const ColumnDefines & columns_to_read,
        AttrCreatorByColumnID && creator,
        Poco::Logger * log);

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

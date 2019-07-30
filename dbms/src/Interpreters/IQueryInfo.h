#pragma once


#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>
#include <Core/QueryProcessingStage.h>

namespace DB
{

/** IQueryInfo interface for different source of queries.
  */
class IQueryInfo
{
public:

    virtual bool isInternalQuery() = 0;
    virtual std::tuple<std::string, ASTPtr> parse(size_t max_query_size) = 0;
    virtual String get_query_ignore_error(size_t max_query_size) = 0;
    virtual std::unique_ptr<IInterpreter> getInterpreter(Context & context, QueryProcessingStage::Enum stage) = 0;
    virtual ~IQueryInfo() {}
};

}

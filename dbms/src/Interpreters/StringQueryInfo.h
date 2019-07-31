#pragma once

#include <Interpreters/IQueryInfo.h>
#include <Parsers/IAST.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{

/** StringQueryInfo for query represented by string.
  */
class StringQueryInfo : public IQueryInfo
{
public:

    StringQueryInfo(const char * begin_, const char * end_, bool internal_);
    std::tuple<std::string, ASTPtr> parse(size_t max_query_size);
    String get_query_ignore_error(size_t max_query_size);
    std::unique_ptr<IInterpreter> getInterpreter(Context & context, QueryProcessingStage::Enum stage);
    bool isInternalQuery() {return internal;};

private:
    const char * begin;
    const char * end;
    bool internal;
    String query;
    ASTPtr ast;
};

}

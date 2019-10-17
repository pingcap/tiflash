#pragma once

#include <Interpreters/IQuerySource.h>

namespace DB
{

/// Regular query source of a SQL string.
class SQLQuerySource : public IQuerySource
{
public:
    SQLQuerySource(const char * begin_, const char * end_);

    std::tuple<std::string, ASTPtr> parse(size_t max_query_size) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

private:
    const char * begin;
    const char * end;
    String query;
    ASTPtr ast;
};

} // namespace DB

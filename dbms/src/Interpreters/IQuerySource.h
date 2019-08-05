#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>

namespace DB
{

/// A tiny abstraction of different sources a query comes from, i.e. SQL string or DAG request.
class IQuerySource
{
public:
    virtual ~IQuerySource() = default;

    virtual std::tuple<std::string, ASTPtr> parse(size_t max_query_size) = 0;
    virtual String str(size_t max_query_size) = 0;
    virtual std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) = 0;
};

} // namespace DB

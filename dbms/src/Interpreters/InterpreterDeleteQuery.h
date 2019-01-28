#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDeleteQuery.h>

namespace DB
{

/** Interprets the DELETE query.
  */
class InterpreterDeleteQuery : public IInterpreter
{
public:
    InterpreterDeleteQuery(const ASTPtr & query_ptr_, const Context & context_, bool allow_materialized_ = false);

    BlockIO execute() override;

private:
    void checkAccess(const ASTDeleteQuery & query);

    ASTPtr query_ptr;
    const Context & context;
    bool allow_materialized;
};

}

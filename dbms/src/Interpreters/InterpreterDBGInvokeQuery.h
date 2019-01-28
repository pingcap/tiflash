#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
class ASTDBGInvokeQuery;
using ASTPtr = std::shared_ptr<IAST>;


/** Invoke inner functions, for debug only.
  */
class InterpreterDBGInvokeQuery : public IInterpreter
{
public:
    InterpreterDBGInvokeQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
};


}

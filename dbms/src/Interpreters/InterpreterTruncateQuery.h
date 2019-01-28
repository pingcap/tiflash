#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class InterpreterTruncateQuery : public IInterpreter
{
public:
  InterpreterTruncateQuery(const ASTPtr & query_ptr_, Context & context_)
      : query_ptr(query_ptr_), context(context_) {}

  BlockIO execute() override;

private:
  ASTPtr query_ptr;
  Context & context;
};


}

#include <Functions/registerFunctionTiDBDivideFloating.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionTiDBDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBDivideFloating>();
}

}

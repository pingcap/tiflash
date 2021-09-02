#include <Functions/registerFunctionTiDBDivideFloating.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>

namespace DB
{

void registerFunctionTiDBDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBDivideFloating>();
}

}

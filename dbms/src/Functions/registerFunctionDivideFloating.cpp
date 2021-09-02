#include <Functions/registerFunctionDivideFloating.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>

namespace DB
{

void registerFunctionDivideFloating(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideFloating>();
}

}

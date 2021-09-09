#include <Functions/registerFunctionDivideIntegral.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>

namespace DB
{

void registerFunctionDivideIntegral(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivideIntegral>();
}

}

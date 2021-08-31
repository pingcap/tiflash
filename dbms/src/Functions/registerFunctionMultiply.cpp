#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/registerFunctionMultiply.h>

namespace DB
{
void registerFunctionMultiply(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiply>();
}

} // namespace DB

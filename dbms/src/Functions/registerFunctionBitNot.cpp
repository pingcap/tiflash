#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/registerFunctionBitNot.h>

namespace DB
{
void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

} // namespace DB

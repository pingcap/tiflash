#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsUnaryArithmetic.h>
#include <Functions/registerFunctionBitNot.h>

namespace DB
{
void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

} // namespace DB

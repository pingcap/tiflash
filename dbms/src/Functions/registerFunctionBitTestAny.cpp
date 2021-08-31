#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/registerFunctionBitTestAny.h>

namespace DB
{
void registerFunctionBitTestAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAny>();
}

} // namespace DB

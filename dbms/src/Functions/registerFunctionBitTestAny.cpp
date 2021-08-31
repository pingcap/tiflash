#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>
#include <Functions/registerFunctionBitTestAny.h>

namespace DB
{
void registerFunctionBitTestAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAny>();
}

} // namespace DB

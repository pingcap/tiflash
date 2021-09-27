#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>
#include <Functions/registerFunctionBitXor.h>

namespace DB
{
void registerFunctionBitXor(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitXor>();
}

} // namespace DB

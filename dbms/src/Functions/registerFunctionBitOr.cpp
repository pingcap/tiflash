#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>
#include <Functions/registerFunctionBitOr.h>

namespace DB
{
void registerFunctionBitOr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitOr>();
}

} // namespace DB

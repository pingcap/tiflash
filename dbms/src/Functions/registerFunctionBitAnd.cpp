#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/registerFunctionBitAnd.h>

namespace DB
{
void registerFunctionBitAnd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitAnd>();
}

} // namespace DB

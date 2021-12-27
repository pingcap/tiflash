#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayUInt64.h>

namespace DB
{
void registerFunctionEmptyArrayUInt64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt64>();
}

} // namespace DB

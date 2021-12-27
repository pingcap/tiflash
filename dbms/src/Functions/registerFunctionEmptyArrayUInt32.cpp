#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayUInt32.h>

namespace DB
{
void registerFunctionEmptyArrayUInt32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt32>();
}

} // namespace DB

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayUInt16.h>

namespace DB
{
void registerFunctionEmptyArrayUInt16(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt16>();
}

} // namespace DB

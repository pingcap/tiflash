#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayUInt8.h>

namespace DB
{
void registerFunctionEmptyArrayUInt8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayUInt8>();
}

} // namespace DB

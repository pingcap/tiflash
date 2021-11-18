#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayEnumerate.h>

namespace DB
{
void registerFunctionArrayEnumerate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerate>();
}

} // namespace DB

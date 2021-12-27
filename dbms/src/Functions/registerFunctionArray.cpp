#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArray.h>

namespace DB
{
void registerFunctionArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArray>();
}

} // namespace DB

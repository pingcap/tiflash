#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayEnumerateUniq.h>

namespace DB
{
void registerFunctionArrayEnumerateUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateUniq>();
}

} // namespace DB

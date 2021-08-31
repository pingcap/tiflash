#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionIndexOf.h>

namespace DB
{
void registerFunctionIndexOf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIndexOf>();
}

} // namespace DB

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayReverse.h>

namespace DB
{
void registerFunctionArrayReverse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReverse>();
}

} // namespace DB

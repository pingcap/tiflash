#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayResize.h>

namespace DB
{
void registerFunctionArrayResize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayResize>();
}

} // namespace DB

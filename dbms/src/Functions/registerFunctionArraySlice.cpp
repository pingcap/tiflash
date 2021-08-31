#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArraySlice.h>

namespace DB
{
void registerFunctionArraySlice(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArraySlice>();
}

} // namespace DB

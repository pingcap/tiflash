#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayInt64.h>

namespace DB
{
void registerFunctionEmptyArrayInt64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt64>();
}

} // namespace DB

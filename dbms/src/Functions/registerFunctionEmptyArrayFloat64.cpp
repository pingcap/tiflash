#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayFloat64.h>

namespace DB
{
void registerFunctionEmptyArrayFloat64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayFloat64>();
}

} // namespace DB

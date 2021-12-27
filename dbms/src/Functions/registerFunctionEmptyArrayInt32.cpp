#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayInt32.h>

namespace DB
{
void registerFunctionEmptyArrayInt32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt32>();
}

} // namespace DB

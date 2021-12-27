#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayInt16.h>

namespace DB
{
void registerFunctionEmptyArrayInt16(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt16>();
}

} // namespace DB

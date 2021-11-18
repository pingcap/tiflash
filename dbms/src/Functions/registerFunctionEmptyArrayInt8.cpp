#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayInt8.h>

namespace DB
{
void registerFunctionEmptyArrayInt8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt8>();
}

} // namespace DB

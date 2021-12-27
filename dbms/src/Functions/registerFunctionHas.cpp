#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionHas.h>

namespace DB
{
void registerFunctionHas(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHas>();
}

} // namespace DB

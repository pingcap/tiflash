#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayDateTime.h>

namespace DB
{
void registerFunctionEmptyArrayDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayDateTime>();
}

} // namespace DB

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayString.h>

namespace DB
{
void registerFunctionEmptyArrayString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayString>();
}

} // namespace DB

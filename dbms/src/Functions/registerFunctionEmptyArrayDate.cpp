#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayDate.h>

namespace DB
{
void registerFunctionEmptyArrayDate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayDate>();
}

} // namespace DB

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayToSingle.h>

namespace DB
{
void registerFunctionEmptyArrayToSingle(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayToSingle>();
}

} // namespace DB

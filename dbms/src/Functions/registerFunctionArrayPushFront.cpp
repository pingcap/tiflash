#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayPushFront.h>

namespace DB
{
void registerFunctionArrayPushFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushFront>();
}

} // namespace DB

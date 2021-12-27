#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayIntersect.h>

namespace DB
{
void registerFunctionArrayIntersect(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayIntersect>();
}

} // namespace DB

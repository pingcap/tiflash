#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayPopFront.h>

namespace DB
{
void registerFunctionArrayPopFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopFront>();
}

} // namespace DB

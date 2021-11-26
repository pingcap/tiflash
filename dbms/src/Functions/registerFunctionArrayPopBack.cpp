#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayPopBack.h>

namespace DB
{
void registerFunctionArrayPopBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopBack>();
}

} // namespace DB

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayConcat.h>

namespace DB
{
void registerFunctionArrayConcat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayConcat>();
}

} // namespace DB

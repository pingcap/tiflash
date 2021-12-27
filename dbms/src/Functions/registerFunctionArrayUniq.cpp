#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayUniq.h>

namespace DB
{
void registerFunctionArrayUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayUniq>();
}

} // namespace DB

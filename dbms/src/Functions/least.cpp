#include <Functions/least.h>

namespace DB
{
void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeast>();
}

} // namespace DB
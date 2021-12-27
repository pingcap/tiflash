#include <Functions/plus.h>

namespace DB
{
void registerFunctionPlus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPlus>();
}

} // namespace DB
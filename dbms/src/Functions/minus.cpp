#include <Functions/minus.h>

namespace DB
{
void registerFunctionMinus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMinus>();
}

} // namespace DB
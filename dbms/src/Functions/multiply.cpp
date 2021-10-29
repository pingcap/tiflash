#include <Functions/multiply.h>

namespace DB
{
void registerFunctionMultiply(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiply>();
}

} // namespace DB
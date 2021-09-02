#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>
#include <Functions/registerFunctionModulo.h>

namespace DB
{
void registerFunctionModulo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModulo>();
}

} // namespace DB

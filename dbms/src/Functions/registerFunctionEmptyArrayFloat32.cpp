#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionEmptyArrayFloat32.h>

namespace DB
{
void registerFunctionEmptyArrayFloat32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayFloat32>();
}

} // namespace DB

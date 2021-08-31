#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/registerFunctionArrayReduce.h>

namespace DB
{
void registerFunctionArrayReduce(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReduce>();
}

} // namespace DB

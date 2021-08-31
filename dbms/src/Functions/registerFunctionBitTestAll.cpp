#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>
#include <Functions/registerFunctionBitTestAll.h>

namespace DB
{
void registerFunctionBitTestAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAll>();
}

} // namespace DB

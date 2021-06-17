#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringMath.h>

namespace DB
{


void registerFunctionsStringMath(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCRC32>();
    factory.registerFunction<FunctionConv>();
}

}
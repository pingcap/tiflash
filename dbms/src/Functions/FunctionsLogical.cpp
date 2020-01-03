#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{

void registerFunctionsLogical(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAnd>();
    //factory.registerFunction<FunctionTiDBAnd>();
    factory.registerFunction<FunctionOr>();
    //factory.registerFunction<FunctionTiDBOr>();
    factory.registerFunction<FunctionXor>();
    factory.registerFunction<FunctionNot>();
}

}

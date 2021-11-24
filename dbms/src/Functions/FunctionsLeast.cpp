#include "Functions/FunctionFactory.h"
#include <Functions/FunctionsLeast.h>
namespace DB
{
void registerFunctionsTiDBLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
}
} // namespace DB

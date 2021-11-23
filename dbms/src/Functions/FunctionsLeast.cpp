#include <Functions/FunctionsLeast.h>
#include "Functions/FunctionFactory.h"

namespace DB
{
void registerFunctionsTiDBLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
}
} // namespace DB

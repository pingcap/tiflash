#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>


namespace DB
{

void registerWindowFunctions(AggregateFunctionFactory & factory);

void registerAggregateFunctions()
{
    auto & factory = AggregateFunctionFactory::instance();
    registerWindowFunctions(factory);
}

} // namespace DB

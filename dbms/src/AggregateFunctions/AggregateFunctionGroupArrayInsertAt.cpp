#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArrayInsertAt.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
namespace
{
AggregateFunctionPtr createAggregateFunctionGroupArrayInsertAt(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    return std::make_shared<AggregateFunctionGroupArrayInsertAtGeneric>(argument_types, parameters);
}

} // namespace

void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupArrayInsertAt", createAggregateFunctionGroupArrayInsertAt);
}

} // namespace DB

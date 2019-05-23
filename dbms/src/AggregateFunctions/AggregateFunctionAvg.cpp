#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;

    const IDataType *p = argument_types[0].get();
    // TODO:: more precise.
    if (auto dec_type = typeid_cast<const DataTypeDecimal32* >(p)) {
        res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal64>(*dec_type, dec_type->getPrec(), dec_type->getScale()));
    } else if (auto dec_type = typeid_cast<const DataTypeDecimal64* >(p)) {
        res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal128>(*dec_type, dec_type->getPrec(), dec_type->getScale()));
    } else if (auto dec_type = typeid_cast<const DataTypeDecimal128* >(p)) {
        res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal256>(*dec_type, dec_type->getPrec(), dec_type->getScale()));
    } else if (auto dec_type = typeid_cast<const DataTypeDecimal256* >(p)) {
        res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal256>(*dec_type, dec_type->getPrec(), dec_type->getScale()));
    } else {
        res = AggregateFunctionPtr(createWithNumericType<AggregateFunctionAvg>(*argument_types[0]));
    }
    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}

}

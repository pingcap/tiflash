#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

template <typename T>
AggregateFunctionPtr createAggregateFunctionAvgForDecimal(const IDataType * p)
{
    if (auto dec_type = typeid_cast<const T *>(p))
    {
        AggregateFunctionPtr res;
        PrecType result_prec;
        ScaleType result_scale;
        AvgDecimalInferer::infer(dec_type->getPrec(), dec_type->getScale(), result_prec, result_scale);
        auto result_type = createDecimal(result_prec, result_scale);
        if (checkDecimal<Decimal32>(*result_type))
            res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal32>(
                *dec_type, dec_type->getPrec(), dec_type->getScale(), result_prec, result_scale));
        else if (checkDecimal<Decimal64>(*result_type))
            res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal64>(
                *dec_type, dec_type->getPrec(), dec_type->getScale(), result_prec, result_scale));
        else if (checkDecimal<Decimal128>(*result_type))
            res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal128>(
                *dec_type, dec_type->getPrec(), dec_type->getScale(), result_prec, result_scale));
        else
            res = AggregateFunctionPtr(createWithDecimalType<AggregateFunctionAvg, Decimal256>(
                *dec_type, dec_type->getPrec(), dec_type->getScale(), result_prec, result_scale));
        return res;
    }
    return nullptr;
}

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;

    const IDataType * p = argument_types[0].get();
    if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal32>(p)) != nullptr)
        return res;
    else if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal64>(p)) != nullptr)
        return res;
    else if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal128>(p)) != nullptr)
        return res;
    else if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal256>(p)) != nullptr)
        return res;
    else
        res = AggregateFunctionPtr(createWithNumericType<AggregateFunctionAvg>(*p));
    if (!res)
        throw Exception(
            "Illegal type " + p->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

} // namespace

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}

} // namespace DB

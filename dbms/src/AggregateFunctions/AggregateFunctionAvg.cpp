// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>

namespace DB
{
namespace
{
template <typename CALCULATETYPE, typename RESULTTYPE>
IAggregateFunction * createAvgWithDecimalType(
    const IDataType & argument_type,
    PrecType arg_prec,
    ScaleType arg_scale,
    PrecType res_prec,
    ScaleType res_scale)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateFunctionAvg<FIELDTYPE, CALCULATETYPE, RESULTTYPE>(arg_prec, arg_scale, res_prec, res_scale);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

IAggregateFunction * createAvgWithNumericType(const IDataType & argument_type)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateFunctionAvg<FIELDTYPE, FIELDTYPE>();
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <typename T>
AggregateFunctionPtr createAggregateFunctionAvgForDecimal(const IDataType * p, Int32 div_precincrement)
{
    if (const auto * dec_type = typeid_cast<const T *>(p))
    {
        AggregateFunctionPtr res;

        auto [calculate_prec, calculate_scale]
            = AvgDecimalInferer::inferCalculate(dec_type->getPrec(), dec_type->getScale(), div_precincrement);
        auto calculate_type = createDecimal(calculate_prec, calculate_scale);

        auto [result_prec, result_scale]
            = AvgDecimalInferer::inferResultType(dec_type->getPrec(), dec_type->getScale(), div_precincrement);
        auto result_type = createDecimal(result_prec, result_scale);

#define DISPATCH(CALCULATETYPE, RESULTTYPE)                                                     \
    if (checkDecimal<CALCULATETYPE>(*calculate_type) && checkDecimal<RESULTTYPE>(*result_type)) \
        return AggregateFunctionPtr(createAvgWithDecimalType<CALCULATETYPE, RESULTTYPE>(        \
            *dec_type,                                                                          \
            dec_type->getPrec(),                                                                \
            dec_type->getScale(),                                                               \
            result_prec,                                                                        \
            result_scale));

        DISPATCH(Decimal128, Decimal32)
        DISPATCH(Decimal128, Decimal64)
        DISPATCH(Decimal128, Decimal128)
        DISPATCH(Decimal256, Decimal32)
        DISPATCH(Decimal256, Decimal64)
        DISPATCH(Decimal256, Decimal128)
        DISPATCH(Decimal256, Decimal256)
#undef DISPATCH
    }
    return nullptr;
}

AggregateFunctionPtr createAggregateFunctionAvg(
    const Context & context,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;

    Int32 div_precincrement = DEFAULT_DIV_PRECISION_INCREMENT;
    if (context.getDAGContext())
    {
        div_precincrement = context.getDAGContext()->getDivPrecisionIncrement();
    }
    const IDataType * p = argument_types[0].get();
    if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal32>(p, div_precincrement)) != nullptr)
        return res;
    else if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal64>(p, div_precincrement)) != nullptr)
        return res;
    else if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal128>(p, div_precincrement)) != nullptr)
        return res;
    else if ((res = createAggregateFunctionAvgForDecimal<DataTypeDecimal256>(p, div_precincrement)) != nullptr)
        return res;
    else
        res = AggregateFunctionPtr(createAvgWithNumericType(*p));
    if (!res)
        throw Exception(
            fmt::format("Illegal type {} of argument for aggregate function {}", p->getName(), name),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

} // namespace

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
}

} // namespace DB

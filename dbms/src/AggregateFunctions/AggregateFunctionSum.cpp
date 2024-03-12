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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <fmt/core.h>

namespace DB
{

extern const String count_second_stage = NameCountSecondStage::name;
extern const String sum_on_partial_result = NameSumOnPartialResult::name;

namespace
{
template <typename T>
using AggregateFunctionSumSimple = AggregateFunctionSum<
    T,
    typename NearestFieldType<T>::Type,
    AggregateFunctionSumData<typename NearestFieldType<T>::Type>>;

template <typename T>
using AggregateFunctionCountSecondStage = AggregateFunctionSum<
    T,
    typename NearestFieldType<T>::Type,
    AggregateFunctionSumData<typename NearestFieldType<T>::Type>,
    NameCountSecondStage>;

template <typename T, typename TResult, typename SumName>
using AggregateFunctionSumDecimal = AggregateFunctionSum<T, TResult, AggregateFunctionSumData<TResult>, SumName>;

template <typename T>
using AggregateFunctionSumWithOverflow = AggregateFunctionSum<T, T, AggregateFunctionSumData<T>, NameSumWithOverFlow>;

template <typename T>
using AggregateFunctionSumOnPartialResultStage = AggregateFunctionSumWithOverflow<T>;

template <typename T>
using AggregateFunctionSumKahan
    = AggregateFunctionSum<T, Float64, AggregateFunctionSumKahanData<Float64>, NameSumKahan>;

template <typename T, typename Name>
AggregateFunctionPtr createDecimalFunction(const IDataType * p)
{
    if (auto dec_type = typeid_cast<const DataTypeDecimal<T> *>(p))
    {
        PrecType prec = dec_type->getPrec();
        ScaleType scale = dec_type->getScale();

        auto [result_prec, result_scale] = Name::decimalInfer(prec, scale);
        auto result_type = createDecimal(result_prec, result_scale);

        if (checkDecimal<Decimal32>(*result_type))
            return AggregateFunctionPtr(
                createSumAggWithDecimalType<AggregateFunctionSumDecimal, Decimal32, Name>(*dec_type, prec, scale));

        if (checkDecimal<Decimal64>(*result_type))
            return AggregateFunctionPtr(
                createSumAggWithDecimalType<AggregateFunctionSumDecimal, Decimal64, Name>(*dec_type, prec, scale));

        if (checkDecimal<Decimal128>(*result_type))
            return AggregateFunctionPtr(
                createSumAggWithDecimalType<AggregateFunctionSumDecimal, Decimal128, Name>(*dec_type, prec, scale));

        if (checkDecimal<Decimal256>(*result_type))
            return AggregateFunctionPtr(
                createSumAggWithDecimalType<AggregateFunctionSumDecimal, Decimal256, Name>(*dec_type, prec, scale));
    }
    return nullptr;
}

template <template <typename> class Function, typename Name>
AggregateFunctionPtr createAggregateFunctionSum(
    const Context & /* context not used */,
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const IDataType * p = argument_types[0].get();

    if ((res = createDecimalFunction<Decimal32, Name>(p)) != nullptr) {}
    else if ((res = createDecimalFunction<Decimal64, Name>(p)) != nullptr)
    {
    }
    else if ((res = createDecimalFunction<Decimal128, Name>(p)) != nullptr)
    {
    }
    else if ((res = createDecimalFunction<Decimal256, Name>(p)) != nullptr)
    {
    }
    else
    {
        res = AggregateFunctionPtr(createWithNumericType<Function>(*p));
    }

    if (!res)
        throw Exception(
            fmt::format("Illegal type {} of argument for aggregate function {}", p->getName(), name),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

} // namespace

void registerAggregateFunctionSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        NameSum::name,
        createAggregateFunctionSum<AggregateFunctionSumSimple, NameSum>,
        AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        NameSumOnPartialResult::name,
        createAggregateFunctionSum<AggregateFunctionSumOnPartialResultStage, NameSumOnPartialResult>);
    factory.registerFunction(NameSumKahan::name, createAggregateFunctionSum<AggregateFunctionSumKahan, NameSumKahan>);
    factory.registerFunction(
        NameCountSecondStage::name,
        createAggregateFunctionSum<AggregateFunctionCountSecondStage, NameCountSecondStage>,
        AggregateFunctionFactory::CaseInsensitive);
}

} // namespace DB

// Copyright 2022 PingCAP, Ltd.
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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionsURL.h>
#include <Functions/FunctionsVisitParam.h>


namespace DB
{
struct NameVisitParamHas
{
    static constexpr auto name = "visitParamHas";
};
struct NameVisitParamExtractUInt
{
    static constexpr auto name = "visitParamExtractUInt";
};
struct NameVisitParamExtractInt
{
    static constexpr auto name = "visitParamExtractInt";
};
struct NameVisitParamExtractFloat
{
    static constexpr auto name = "visitParamExtractFloat";
};
struct NameVisitParamExtractBool
{
    static constexpr auto name = "visitParamExtractBool";
};
struct NameVisitParamExtractRaw
{
    static constexpr auto name = "visitParamExtractRaw";
};
struct NameVisitParamExtractString
{
    static constexpr auto name = "visitParamExtractString";
};


using FunctionVisitParamHas = FunctionsStringSearch<ExtractParamImpl<HasParam>, NameVisitParamHas>;
using FunctionVisitParamExtractUInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<UInt64>>, NameVisitParamExtractUInt>;
using FunctionVisitParamExtractInt = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64>>, NameVisitParamExtractInt>;
using FunctionVisitParamExtractFloat = FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64>>, NameVisitParamExtractFloat>;
using FunctionVisitParamExtractBool = FunctionsStringSearch<ExtractParamImpl<ExtractBool>, NameVisitParamExtractBool>;
using FunctionVisitParamExtractRaw = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractRaw>, NameVisitParamExtractRaw>;
using FunctionVisitParamExtractString = FunctionsStringSearchToString<ExtractParamToStringImpl<ExtractString>, NameVisitParamExtractString>;


void registerFunctionsVisitParam(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisitParamHas>();
    factory.registerFunction<FunctionVisitParamExtractUInt>();
    factory.registerFunction<FunctionVisitParamExtractInt>();
    factory.registerFunction<FunctionVisitParamExtractFloat>();
    factory.registerFunction<FunctionVisitParamExtractBool>();
    factory.registerFunction<FunctionVisitParamExtractRaw>();
    factory.registerFunction<FunctionVisitParamExtractString>();
}

} // namespace DB

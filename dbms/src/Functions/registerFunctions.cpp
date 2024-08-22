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

#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>


namespace DB
{
/** These functions are defined in a separate translation units.
  * This is done in order to reduce the consumption of RAM during build, and to speed up the parallel build.
  */
void registerFunctionsArithmetic(FunctionFactory &);
void registerFunctionsTuple(FunctionFactory &);
void registerFunctionsCoding(FunctionFactory &);
void registerFunctionsComparison(FunctionFactory &);
void registerFunctionsConditional(FunctionFactory &);
void registerFunctionsConversion(FunctionFactory &);
void registerFunctionsTiDBConversion(FunctionFactory &);
void registerFunctionsDateTime(FunctionFactory &);
void registerFunctionsHashing(FunctionFactory &);
void registerFunctionsLogical(FunctionFactory &);
void registerFunctionsMiscellaneous(FunctionFactory &);
void registerFunctionsRound(FunctionFactory &);
void registerFunctionsString(FunctionFactory &);
void registerFunctionsStringSearch(FunctionFactory &);
void registerFunctionsURL(FunctionFactory &);
void registerFunctionsMath(FunctionFactory &);
void registerFunctionsTransform(FunctionFactory &);
void registerFunctionsGeo(FunctionFactory &);
void registerFunctionsNull(FunctionFactory &);
void registerFunctionsStringMath(FunctionFactory &);
void registerFunctionsDuration(FunctionFactory &);
void registerFunctionsRegexp(FunctionFactory &);
void registerFunctionsJson(FunctionFactory &);
void registerFunctionsIsIPAddr(FunctionFactory &);
void registerFunctionsRegexpLike(FunctionFactory &);
void registerFunctionsRegexpInstr(FunctionFactory &);
void registerFunctionsRegexpSubstr(FunctionFactory &);
void registerFunctionsRegexpReplace(FunctionFactory &);
void registerFunctionsGrouping(FunctionFactory &);
void registerFunctionsVector(FunctionFactory &);

void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    registerFunctionsArithmetic(factory);
    registerFunctionsTuple(factory);
    registerFunctionsCoding(factory);
    registerFunctionsComparison(factory);
    registerFunctionsConditional(factory);
    registerFunctionsConversion(factory);
    registerFunctionsTiDBConversion(factory);
    registerFunctionsDateTime(factory);
    registerFunctionsHashing(factory);
    registerFunctionsLogical(factory);
    registerFunctionsMiscellaneous(factory);
    registerFunctionsRound(factory);
    registerFunctionsString(factory);
    registerFunctionsStringSearch(factory);
    registerFunctionsURL(factory);
    registerFunctionsMath(factory);
    registerFunctionsTransform(factory);
    registerFunctionsGeo(factory);
    registerFunctionsNull(factory);
    registerFunctionsStringMath(factory);
    registerFunctionsDuration(factory);
    registerFunctionsRegexpLike(factory);
    registerFunctionsRegexpInstr(factory);
    registerFunctionsRegexpSubstr(factory);
    registerFunctionsRegexpReplace(factory);
    registerFunctionsJson(factory);
    registerFunctionsIsIPAddr(factory);
    registerFunctionsGrouping(factory);
    registerFunctionsVector(factory);
}

} // namespace DB

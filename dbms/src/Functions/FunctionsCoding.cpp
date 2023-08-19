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
#include <Functions/FunctionsCoding.h>

namespace DB
{
struct NameFunctionIPv4NumToString
{
    static constexpr auto name = "IPv4NumToString";
};
struct NameFunctionIPv4NumToStringClassC
{
    static constexpr auto name = "IPv4NumToStringClassC";
};


void registerFunctionsCoding(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutIPv6>();
    factory.registerFunction<FunctionIPv6StringToNum>();
    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>();
    factory.registerFunction<FunctionIPv4NumToString<1, NameFunctionIPv4NumToStringClassC>>();
    factory.registerFunction<FunctionIPv4StringToNum>();
    factory.registerFunction<FunctionTiDBIPv4StringToNum>();
    factory.registerFunction<FunctionTiDBIPv6StringToNum>();
    factory.registerFunction<FunctionTiDBIPv6NumToString>();
    factory.registerFunction<FunctionIPv4ToIPv6>();
    factory.registerFunction<FunctionHex>();
    factory.registerFunction<FunctionUnhex>();
}

} // namespace DB

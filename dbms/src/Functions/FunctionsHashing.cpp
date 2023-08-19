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
#include <Functions/FunctionsHashing.h>


namespace DB
{
void registerFunctionsHashing(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHalfMD5>();
    factory.registerFunction<FunctionMD5>();
    factory.registerFunction<FunctionSHA1>();
    factory.registerFunction<FunctionSHA224>();
    factory.registerFunction<FunctionSHA256>();
    factory.registerFunction<FunctionSipHash64>();
    factory.registerFunction<FunctionSipHash128>();
    factory.registerFunction<FunctionCityHash64>();
    factory.registerFunction<FunctionFarmHash64>();
    factory.registerFunction<FunctionMetroHash64>();
    factory.registerFunction<FunctionIntHash32>();
    factory.registerFunction<FunctionIntHash64>();
    factory.registerFunction<FunctionURLHash>();
}

template <>
UInt64 toInteger<Float32>(Float32 x)
{
    UInt32 res;
    memcpy(&res, &x, sizeof(x));
    return res;
}

template <>
UInt64 toInteger<Float64>(Float64 x)
{
    UInt64 res;
    memcpy(&res, &x, sizeof(x));
    return res;
}

} // namespace DB

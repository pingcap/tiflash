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
#include <Functions/FunctionsMath.h>

namespace DB
{
const double EImpl::value = 2.7182818284590452353602874713526624977572470;
const double PiImpl::value = 3.1415926535897932384626433832795028841971693;


void registerFunctionsMath(FunctionFactory & factory)
{
    factory.registerFunction<FunctionE>();
    factory.registerFunction<FunctionPi>();
    factory.registerFunction<FunctionSign>();
    factory.registerFunction<FunctionRadians>();
    factory.registerFunction<FunctionDegrees>();
    factory.registerFunction<FunctionExp>();
    factory.registerFunction<FunctionLog>();
    factory.registerFunction<FunctionLog2Args>();
    factory.registerFunction<FunctionExp2>();
    factory.registerFunction<FunctionLog2>();
    factory.registerFunction<FunctionExp10>();
    factory.registerFunction<FunctionLog10>();
    factory.registerFunction<FunctionSqrt>();
    factory.registerFunction<FunctionCbrt>();
    factory.registerFunction<FunctionErf>();
    factory.registerFunction<FunctionErfc>();
    factory.registerFunction<FunctionLGamma>();
    factory.registerFunction<FunctionTGamma>();
    factory.registerFunction<FunctionSin>();
    factory.registerFunction<FunctionCos>();
    factory.registerFunction<FunctionTan>();
    factory.registerFunction<FunctionAsin>();
    factory.registerFunction<FunctionAcos>();
    factory.registerFunction<FunctionAtan>();
    factory.registerFunction<FunctionPow>();
}

} // namespace DB

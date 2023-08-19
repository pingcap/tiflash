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
#include <Functions/FunctionsRound.h>

namespace DB
{
void registerFunctionsRound(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundToExp2>();
    factory.registerFunction<FunctionRoundDuration>();
    factory.registerFunction<FunctionRoundAge>();

    factory.registerFunction<FunctionRound>("round", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionFloor>("floor", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCeil>("ceil", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("trunc", FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionRoundDecimalToInt>();
    factory.registerFunction<FunctionCeilDecimalToInt>();
    factory.registerFunction<FunctionFloorDecimalToInt>();
    factory.registerFunction<FunctionTruncDecimalToInt>();

    /// Compatibility aliases.
    factory.registerFunction<FunctionCeil>("ceiling", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("truncate", FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionTiDBRoundWithFrac>();
}

} // namespace DB

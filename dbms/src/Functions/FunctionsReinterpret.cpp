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
#include <Functions/FunctionsReinterpret.h>

namespace DB
{
void registerFunctionsReinterpret(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReinterpretAsUInt8>();
    factory.registerFunction<FunctionReinterpretAsUInt16>();
    factory.registerFunction<FunctionReinterpretAsUInt32>();
    factory.registerFunction<FunctionReinterpretAsUInt64>();
    factory.registerFunction<FunctionReinterpretAsInt8>();
    factory.registerFunction<FunctionReinterpretAsInt16>();
    factory.registerFunction<FunctionReinterpretAsInt32>();
    factory.registerFunction<FunctionReinterpretAsInt64>();
    factory.registerFunction<FunctionReinterpretAsFloat32>();
    factory.registerFunction<FunctionReinterpretAsFloat64>();
    factory.registerFunction<FunctionReinterpretAsDate>();
    factory.registerFunction<FunctionReinterpretAsDateTime>();
    factory.registerFunction<FunctionReinterpretAsString>();
    factory.registerFunction<FunctionReinterpretAsFixedString>();
}

} // namespace DB

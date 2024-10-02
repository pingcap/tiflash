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
#include <Functions/FunctionsBinaryLogical.h>
#include <Functions/FunctionsLogical.h>

namespace DB
{
void registerFunctionsLogical(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAnd>();
    factory.registerFunction<FunctionOr>();
    factory.registerFunction<FunctionXor>();
    factory.registerFunction<FunctionNot>();
    factory.registerFunction<FunctionBinaryAnd>();
    factory.registerFunction<FunctionBinaryOr>();
    factory.registerFunction<FunctionBinaryXor>();
}

} // namespace DB

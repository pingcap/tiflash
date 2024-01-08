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
#include <Functions/FunctionsJson.h>

namespace DB
{
void registerFunctionsJson(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJsonExtract>();
    factory.registerFunction<FunctionJsonUnquote>();
    factory.registerFunction<FunctionCastJsonAsString>();
    factory.registerFunction<FunctionJsonLength>();
    factory.registerFunction<FunctionJsonArray>();
    factory.registerFunction<FunctionCastJsonAsJson>();
    factory.registerFunction<FunctionCastRealAsJson>();
    factory.registerFunction<FunctionCastDecimalAsJson>();
    factory.registerFunction<FunctionCastIntAsJson>();
    factory.registerFunction<FunctionCastStringAsJson>();
    factory.registerFunction<FunctionCastTimeAsJson>();
    factory.registerFunction<FunctionCastDurationAsJson>();
    factory.registerFunction<FunctionJsonDepth>();
    factory.registerFunction<FunctionJsonContainsPath>();
    factory.registerFunction<FunctionJsonValidOthers>();
    factory.registerFunction<FunctionJsonValidJson>();
    factory.registerFunction<FunctionJsonValidString>();
    factory.registerFunction<FunctionJsonKeys>();
    factory.registerFunction<FunctionJsonKeys2Args>();
}
} // namespace DB

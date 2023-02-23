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

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRegexp.h>
#include <Functions/Regexps.h>
#include <fmt/core.h>

namespace DB
{
using FunctionTiDBRegexp = FunctionStringRegexp<NameTiDBRegexp>;
using FunctionRegexpLike = FunctionStringRegexp<NameRegexpLike>;
using FunctionRegexpInstr = FunctionStringRegexpInstr<NameRegexpInstr>;
using FunctionRegexpSubstr = FunctionStringRegexpSubstr<NameRegexpSubstr>;
using FunctionRegexpReplace = FunctionStringRegexpReplace<NameRegexpReplace>;

void registerFunctionsRegexp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBRegexp>();
    factory.registerFunction<FunctionRegexpLike>();
    factory.registerFunction<FunctionRegexpInstr>();
    factory.registerFunction<FunctionRegexpSubstr>();
    factory.registerFunction<FunctionRegexpReplace>();
}
} // namespace DB

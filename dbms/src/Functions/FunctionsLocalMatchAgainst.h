// Copyright 2026 PingCAP, Inc.
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

#pragma once

#include <Functions/FunctionFactory.h>

namespace DB
{
void registerFunctionsLocalMatchAgainst(FunctionFactory & factory);

// Internal test seam: no SQL setting, global state, or request-shared scratch.
namespace LocalMatchAgainstTest
{
enum class Matcher
{
    Automatic,
    Linear,
    Indexed
};
struct Work
{
    size_t route_builds = 0;
    size_t indexed_columns = 0;
    size_t linear_columns = 0;
    size_t key_lookups = 0;
    size_t term_references = 0;
    size_t unfiltered_column_analyses = 0;
};
FunctionPtr create(Matcher matcher, Work & work, size_t budget = 4 * 1024 * 1024, UInt32 generation = 0);
} // namespace LocalMatchAgainstTest
} // namespace DB

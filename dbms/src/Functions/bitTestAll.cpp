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

#include <Functions/FunctionBitTestMany.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct BitTestAllImpl
{
    template <typename A, typename B>
    static UInt8 apply(A a, B b)
    {
        return (a & b) == b;
    };
};

// clang-format off
struct NameBitTestAll           { static constexpr auto name = "bitTestAll"; };
// clang-format on

using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

} // namespace

void registerFunctionBitTestAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAll>();
}

} // namespace DB
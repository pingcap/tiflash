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

#include <Common/typeid_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>

namespace DB
{
void registerFunctionsComparison(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEquals>();
    factory.registerFunction<FunctionNotEquals>();
    factory.registerFunction<FunctionLess>();
    factory.registerFunction<FunctionGreater>();
    factory.registerFunction<FunctionLessOrEquals>();
    factory.registerFunction<FunctionGreaterOrEquals>();
    factory.registerFunction<FunctionStrcmp>();
    factory.registerFunction<FunctionIsTrue>();
    factory.registerFunction<FunctionIsTrueWithNull>();
    factory.registerFunction<FunctionIsFalse>();
    factory.registerFunction<FunctionIsFalseWithNull>();
}

template <>
void FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleEqualityImpl<FunctionComparison<EqualsOp, NameEquals>, FunctionAnd>(
        block,
        result,
        x,
        y,
        tuple_size);
}

template <>
void FunctionComparison<NotEqualsOp, NameNotEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleEqualityImpl<FunctionComparison<NotEqualsOp, NameNotEquals>, FunctionOr>(
        block,
        result,
        x,
        y,
        tuple_size);
}

template <>
void FunctionComparison<LessOp, NameLess>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<FunctionComparison<LessOp, NameLess>, FunctionComparison<LessOp, NameLess>>(
        block,
        result,
        x,
        y,
        tuple_size);
}

template <>
void FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<GreaterOp, NameGreater>,
        FunctionComparison<GreaterOp, NameGreater>>(block, result, x, y, tuple_size);
}

template <>
void FunctionComparison<LessOrEqualsOp, NameLessOrEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<LessOp, NameLess>,
        FunctionComparison<LessOrEqualsOp, NameLessOrEquals>>(block, result, x, y, tuple_size);
}

template <>
void FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<GreaterOp, NameGreater>,
        FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>>(block, result, x, y, tuple_size);
}

template <>
void FunctionComparison<CmpOp, NameStrcmp, StrcmpReturnColumnType>::executeTupleImpl(
    [[maybe_unused]] Block & block,
    [[maybe_unused]] size_t result,
    [[maybe_unused]] const ColumnsWithTypeAndName & x,
    [[maybe_unused]] const ColumnsWithTypeAndName & y,
    [[maybe_unused]] size_t tuple_size) const
{
    throw DB::Exception("Strcmp can not be used with tuple");
}

} // namespace DB

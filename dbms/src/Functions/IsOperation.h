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

#pragma once

#include <Common/Decimal.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct PlusImpl;
template <typename A, typename B>
using PlusImpl_t = PlusImpl<A, B>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct MinusImpl;
template <typename A, typename B>
using MinusImpl_t = MinusImpl<A, B>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct MultiplyImpl;
template <typename A, typename B>
using MultiplyImpl_t = MultiplyImpl<A, B>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct DivideFloatingImpl;
template <typename A, typename B>
using DivideFloatingImpl_t = DivideFloatingImpl<A, B>;
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct TiDBDivideFloatingImpl;
template <typename A, typename B>
using TiDBDivideFloatingImpl_t = TiDBDivideFloatingImpl<A, B>;
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct DivideIntegralImpl;
template <typename A, typename B>
using DivideIntegralImpl_t = DivideIntegralImpl<A, B>;
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct DivideIntegralOrZeroImpl;
template <typename A, typename B>
using DivideIntegralOrZeroImpl_t = DivideIntegralOrZeroImpl<A, B>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct ModuloImpl;
template <typename A, typename B>
using ModuloImpl_t = ModuloImpl<A, B>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct BinaryGreatestBaseImpl;
template <typename A, typename B>
using BinaryGreatestBaseImpl_t = BinaryGreatestBaseImpl<A, B>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct BinaryLeastBaseImpl;
template <typename A, typename B>
using BinaryLeastBaseImpl_t = BinaryLeastBaseImpl<A, B>;


template <template <typename, typename> typename Op1, template <typename, typename> typename Op2>
struct IsSameOperation
{
    using SameType = UInt8;
    static constexpr bool value = std::is_same_v<Op1<SameType, SameType>, Op2<SameType, SameType>>;
};

template <template <typename, typename> typename Op>
struct IsOperation
{
    static constexpr bool plus = IsSameOperation<Op, PlusImpl_t>::value;
    static constexpr bool minus = IsSameOperation<Op, MinusImpl_t>::value;
    static constexpr bool multiply = IsSameOperation<Op, MultiplyImpl_t>::value;
    static constexpr bool modulo = IsSameOperation<Op, ModuloImpl_t>::value;
    static constexpr bool div_floating
        = IsSameOperation<Op, DivideFloatingImpl_t>::value || IsSameOperation<Op, TiDBDivideFloatingImpl_t>::value;
    static constexpr bool div_int
        = IsSameOperation<Op, DivideIntegralImpl_t>::value || IsSameOperation<Op, DivideIntegralOrZeroImpl_t>::value;
    static constexpr bool least = IsSameOperation<Op, BinaryLeastBaseImpl_t>::value;
    static constexpr bool greatest = IsSameOperation<Op, BinaryGreatestBaseImpl_t>::value;
};

} // namespace DB
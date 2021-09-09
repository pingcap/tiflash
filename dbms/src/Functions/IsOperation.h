#pragma once

namespace DB
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct PlusImpl;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct MinusImpl;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct MultiplyImpl;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct DivideFloatingImpl;
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct TiDBDivideFloatingImpl;
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct DivideIntegralImpl;
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct DivideIntegralOrZeroImpl;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct ModuloImpl;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct GreatestBaseImpl;
template <typename A, typename B>
struct GreatestSpecialImpl;
template <typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;

template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct LeastBaseImpl;
template <typename A, typename B>
struct LeastSpecialImpl;
template <typename A, typename B>
using LeastImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, LeastBaseImpl<A, B>, LeastSpecialImpl<A, B>>;

template <template <typename, typename> typename Op1, template <typename, typename> typename Op2>
struct IsSameOperation
{
    static constexpr bool value = std::is_same_v<Op1<UInt8, UInt8>, Op2<UInt8, UInt8>>;
};

template <template <typename, typename> typename Op>
struct IsOperation
{
    static constexpr bool plus = IsSameOperation<Op, PlusImpl>::value;
    static constexpr bool minus = IsSameOperation<Op, MinusImpl>::value;
    static constexpr bool multiply = IsSameOperation<Op, MultiplyImpl>::value;
    static constexpr bool modulo = IsSameOperation<Op, ModuloImpl>::value;
    static constexpr bool div_floating = IsSameOperation<Op, DivideFloatingImpl>::value || IsSameOperation<Op, TiDBDivideFloatingImpl>::value;
    static constexpr bool div_int = IsSameOperation<Op, DivideIntegralImpl>::value || IsSameOperation<Op, DivideIntegralOrZeroImpl>::value;
    static constexpr bool least = IsSameOperation<Op, LeastBaseImpl>::value;
    static constexpr bool greatest = IsSameOperation<Op, GreatestBaseImpl>::value;
};

} // namespace DB
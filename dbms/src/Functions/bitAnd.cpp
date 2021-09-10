#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
struct BitAndImpl;

template <typename A, typename B>
struct BitAndImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) & static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BitAndImpl<A, B, true>
{
    using ResultType = If<std::is_unsigned_v<A> || std::is_unsigned_v<B>, uint64_t, int64_t>;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        Result x, y;
        if constexpr (IsDecimal<A>)
        {
            x = static_cast<Result>(a.value);
        }
        else
        {
            x = static_cast<Result>(a);
        }
        if constexpr (IsDecimal<B>)
        {
            y = static_cast<Result>(b.value);
        }
        else
        {
            y = static_cast<Result>(b);
        }
        return BitAndImpl<Result, Result>::apply(x, y);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

// clang-format off
struct NameBitAnd               { static constexpr auto name = "bitAnd"; };
// clang-format on

using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl, NameBitAnd>;

} // namespace

void registerFunctionBitAnd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitAnd>();
}

} // namespace DB
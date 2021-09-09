#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
    struct BitRotateRightImpl;

template <typename A, typename B>
struct BitRotateRightImpl<A, B, false>
    {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
        static Result apply(A a, B b)
        {
            return (static_cast<Result>(a) >> static_cast<Result>(b))
            | (static_cast<Result>(a) << ((sizeof(Result) * 8) - static_cast<Result>(b)));
        }
        template <typename Result = ResultType>
            static Result apply(A, B, UInt8 &)
            {
                throw Exception("Should not reach here");
            }
    };

template <typename A, typename B>
struct BitRotateRightImpl<A, B, true>
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
            return BitRotateRightImpl<Result, Result>::apply(x, y);
        }
        template <typename Result = ResultType>
            static Result apply(A, B, UInt8 &)
            {
                throw Exception("Should not reach here");
            }
    };

// clang-format off
struct NameBitRotateRight       { static constexpr auto name = "bitRotateRight"; };
// clang-format on

using FunctionBitRotateRight = FunctionBinaryArithmetic<BitRotateRightImpl, NameBitRotateRight>;

} // namespace

void registerFunctionBitRotateRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateRight>();
}

} // namespace DB
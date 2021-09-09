#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace
{
template <typename A, typename B, bool existDecimal = IsDecimal<A> || IsDecimal<B>>
    struct BitShiftRightImpl;

template <typename A, typename B>
struct BitShiftRightImpl<A, B, false>
    {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
        static Result apply(A a, B b)
        {
            return static_cast<Result>(a) >> static_cast<Result>(b);
        }
        template <typename Result = ResultType>
            static Result apply(A, B, UInt8 &)
            {
                throw Exception("Should not reach here");
            }
    };

template <typename A, typename B>
struct BitShiftRightImpl<A, B, true>
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
            return BitShiftRightImpl<Result, Result>::apply(x, y);
        }
        template <typename Result = ResultType>
            static Result apply(A, B, UInt8 &)
            {
                throw Exception("Should not reach here");
            }
    };

// clang-format off
struct NameBitShiftRight        { static constexpr auto name = "bitShiftRight"; };
// clang-format on

using FunctionBitShiftRight = FunctionBinaryArithmetic<BitShiftRightImpl, NameBitShiftRight>;

} // namespace

void registerFunctionBitShiftRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftRight>();
}

} // namespace DB
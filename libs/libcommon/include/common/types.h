#pragma once

#include <cstdint>
#include <string>

/// import UInt128 and UInt256
#include <common/UInt128.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#if defined(__clang__)
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#pragma GCC diagnostic ignored "-Wdeprecated-copy"
#endif
#include <boost/multiprecision/cpp_int.hpp>
#pragma GCC diagnostic pop

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int128 = __int128_t;
using Int256 = boost::multiprecision::checked_int256_t;
using Int512 = boost::multiprecision::checked_int512_t;

using String = std::string;

namespace DB
{

using Int8 = ::Int8;
using Int16 = ::Int16;
using Int32 = ::Int32;
using Int64 = ::Int64;

using UInt8 = ::UInt8;
using UInt16 = ::UInt16;
using UInt32 = ::UInt32;
using UInt64 = ::UInt64;

using Float32 = float;
using Float64 = double;

using String = ::String;

using Int128 = ::Int128;
using Int256 = ::Int256;
using Int512 = ::Int512;

} // namespace DB

// Antipattern
using UInt128 = DB::UInt128;
using UInt256 = DB::UInt256;

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined behavior.
/// So instead of using the std type_traits, we use our own version which allows extension.
template <typename T>
struct is_signed
{
    static constexpr bool value = std::is_signed_v<T>;
};

template <> struct is_signed<Int128> { static constexpr bool value = true; };
template <> struct is_signed<Int256> { static constexpr bool value = true; };
template <> struct is_signed<Int512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned
{
    static constexpr bool value = std::is_unsigned_v<T>;
};

template <> struct is_unsigned<UInt128> { static constexpr bool value = true; };
template <> struct is_unsigned<UInt256> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;


/// TODO: is_integral includes char, char8_t and wchar_t.
template <typename T>
struct is_integer
{
    static constexpr bool value = std::is_integral_v<T>;
};

template <> struct is_integer<UInt128> { static constexpr bool value = true; };
template <> struct is_integer<Int128> { static constexpr bool value = true; };
template <> struct is_integer<UInt256> { static constexpr bool value = true; };
template <> struct is_integer<Int256> { static constexpr bool value = true; };
template <> struct is_integer<Int512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_integer_v = is_integer<T>::value;


template <typename T>
struct is_arithmetic
{
    static constexpr bool value = std::is_arithmetic_v<T>;
};

/// UInt128 and UInt256 don't support arithmetic operators.
template <> struct is_arithmetic<Int128> { static constexpr bool value = true; };
template <> struct is_arithmetic<Int256> { static constexpr bool value = true; };
template <> struct is_arithmetic<Int512> { static constexpr bool value = true; };


template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

template <typename T>
struct make_unsigned
{
    typedef std::make_unsigned_t<T> type;
};

template <> struct make_unsigned<UInt128> { using type = UInt128; };
template <> struct make_unsigned<UInt256> { using type = UInt256; };

template <typename T> using make_unsigned_t = typename make_unsigned<T>::type;

template <typename T>
struct make_signed
{
    typedef std::make_signed_t<T> type;
};

template <> struct make_signed<Int128>  { using type = Int128; };
template <> struct make_signed<Int256>  { using type = Int256; };
template <> struct make_signed<Int512>  { using type = Int512; };

template <typename T> using make_signed_t = typename make_signed<T>::type;

template <typename T>
struct is_big_int
{
    static constexpr bool value = false;
};

template <> struct is_big_int<Int128> { static constexpr bool value = true; };
template <> struct is_big_int<UInt128> { static constexpr bool value = true; };
template <> struct is_big_int<Int256> { static constexpr bool value = true; };
template <> struct is_big_int<UInt256> { static constexpr bool value = true; };
template <> struct is_big_int<Int512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;

template <typename T>
struct is_boost_number
{
    static constexpr bool value = false;
};

template <> struct is_boost_number<Int256> { static constexpr bool value = true; };
template <> struct is_boost_number<Int512> { static constexpr bool value = true; };

template <typename T>
inline constexpr bool is_boost_number_v = is_boost_number<T>::value;

template <typename T>
inline constexpr bool is_fit_register = sizeof(T) <= sizeof(UInt64);

/** This is not the best way to overcome an issue of different definitions
  * of uint64_t and size_t on Linux and Mac OS X (both 64 bit).
  *
  * Note that on both platforms, long and long long are 64 bit types.
  * But they are always different types (with the same physical representation).
  */
namespace std
{
inline UInt64 max(unsigned long x, unsigned long long y) { return x > y ? x : y; }
inline UInt64 max(unsigned long long x, unsigned long y) { return x > y ? x : y; }
inline UInt64 min(unsigned long x, unsigned long long y) { return x < y ? x : y; }
inline UInt64 min(unsigned long long x, unsigned long y) { return x < y ? x : y; }

inline Int64 max(long x, long long y) { return x > y ? x : y; }
inline Int64 max(long long x, long y) { return x > y ? x : y; }
inline Int64 min(long x, long long y) { return x < y ? x : y; }
inline Int64 min(long long x, long y) { return x < y ? x : y; }
} // namespace std


/// Workaround for the issue, that KDevelop doesn't see time_t and size_t types (for syntax highlight).
#ifdef IN_KDEVELOP_PARSER
    using time_t = Int64;
    using size_t = UInt64;
#endif

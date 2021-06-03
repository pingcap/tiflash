#pragma once

/// Remove the population of thread_local from Poco
#ifdef thread_local
#   undef thread_local
#endif

#include <limits>

#if !defined(__GLIBCXX_BITSIZE_INT_N_0) && defined(__SIZEOF_INT128__)
namespace std
{
template <>
struct numeric_limits<__int128_t>
{
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = true;
    static constexpr bool is_integer = true;
    static constexpr int radix = 2;
    static constexpr int digits = 128;
    static constexpr __int128_t min() { return __uint128_t(1) << 127; }               // used in boost 1.65.1+
    static constexpr __int128_t max() { return __uint128_t(-1)>>1; } // used in boost 1.65.1+
};
} // namespace std
#endif

#include <string>
#include <vector>
#include <Poco/Types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#if defined(__clang__)
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#pragma GCC diagnostic ignored "-Wdeprecated-copy"
#endif
#include <boost/multiprecision/cpp_int.hpp>
#pragma GCC diagnostic pop

namespace DB
{

/// Data types for representing elementary values from a database in RAM.

struct Null {};

using UInt8 = Poco::UInt8;
using UInt16 = Poco::UInt16;
using UInt32 = Poco::UInt32;
using UInt64 = Poco::UInt64;

using Int8 = Poco::Int8;
using Int16 = Poco::Int16;
using Int32 = Poco::Int32;
using Int64 = Poco::Int64;
using Int128 = __int128_t;
using Int256 = boost::multiprecision::checked_int256_t;
using Int512 = boost::multiprecision::checked_int512_t;

template <typename DataType> constexpr bool IsBoostNumber = false;
template <class Backend, boost::multiprecision::expression_template_option ExpressionTemplates>
inline constexpr bool IsBoostNumber<boost::multiprecision::number<Backend, ExpressionTemplates>>  = true;

template <typename DataType> constexpr bool IsCppIntBackend = false;
template <unsigned MinBits, unsigned MaxBits, boost::multiprecision::cpp_integer_type signed_magnitude, boost::multiprecision::cpp_int_check_type Checked, class Allocator>
inline constexpr bool IsCppIntBackend<boost::multiprecision::cpp_int_backend<MinBits, MaxBits, signed_magnitude, Checked, Allocator>>  = true;

using Float32 = float;
using Float64 = double;

using String = std::string;


/** Note that for types not used in DB, IsNumber is false.
  */
template <typename T> constexpr bool IsNumber = false;

template <> inline constexpr bool IsNumber<UInt8> = true;
template <> inline constexpr bool IsNumber<UInt16> = true;
template <> inline constexpr bool IsNumber<UInt32> = true;
template <> inline constexpr bool IsNumber<UInt64> = true;
template <> inline constexpr bool IsNumber<Int8> = true;
template <> inline constexpr bool IsNumber<Int16> = true;
template <> inline constexpr bool IsNumber<Int32> = true;
template <> inline constexpr bool IsNumber<Int64> = true;
template <> inline constexpr bool IsNumber<Int128> = true;
template <> inline constexpr bool IsNumber<Int256> = true;
template <> inline constexpr bool IsNumber<Int512> = true;
template <> inline constexpr bool IsNumber<Float32> = true;
template <> inline constexpr bool IsNumber<Float64> = true;

template <typename T> struct TypeName;

template <> struct TypeName<UInt8>   { static const char * get() { return "UInt8";   } };
template <> struct TypeName<UInt16>  { static const char * get() { return "UInt16";  } };
template <> struct TypeName<UInt32>  { static const char * get() { return "UInt32";  } };
template <> struct TypeName<UInt64>  { static const char * get() { return "UInt64";  } };
template <> struct TypeName<Int8>    { static const char * get() { return "Int8";    } };
template <> struct TypeName<Int16>   { static const char * get() { return "Int16";   } };
template <> struct TypeName<Int32>   { static const char * get() { return "Int32";   } };
template <> struct TypeName<Int64>   { static const char * get() { return "Int64";   } };
template <> struct TypeName<Int128>  { static const char * get() { return "Int128";  } };
template <> struct TypeName<Int256>  { static const char * get() { return "Int256";  } };
template <> struct TypeName<Int512>  { static const char * get() { return "Int512";  } };
template <> struct TypeName<Float32> { static const char * get() { return "Float32"; } };
template <> struct TypeName<Float64> { static const char * get() { return "Float64"; } };
template <> struct TypeName<String>  { static const char * get() { return "String";  } };


/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;

enum class TypeIndex
{
    Nothing = 0,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Date,
    DateTime,
    String,
    FixedString,
    Enum8,
    Enum16,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    UUID,
    Array,
    Tuple,
    Set,
    Interval,
    Nullable,
    Function,
    AggregateFunction,
    LowCardinality,
    MyDate,
    MyDateTime,
    MyTimeStamp,
    MyTime
};

template <typename T> struct TypeId;
template <> struct TypeId<UInt8>    { static constexpr const TypeIndex value = TypeIndex::UInt8;  };
template <> struct TypeId<UInt16>   { static constexpr const TypeIndex value = TypeIndex::UInt16;  };
template <> struct TypeId<UInt32>   { static constexpr const TypeIndex value = TypeIndex::UInt32;  };
template <> struct TypeId<UInt64>   { static constexpr const TypeIndex value = TypeIndex::UInt64;  };
template <> struct TypeId<Int8>     { static constexpr const TypeIndex value = TypeIndex::Int8;  };
template <> struct TypeId<Int16>    { static constexpr const TypeIndex value = TypeIndex::Int16; };
template <> struct TypeId<Int32>    { static constexpr const TypeIndex value = TypeIndex::Int32; };
template <> struct TypeId<Int64>    { static constexpr const TypeIndex value = TypeIndex::Int64; };
template <> struct TypeId<Int128>    { static constexpr const TypeIndex value = TypeIndex::Int128; };
template <> struct TypeId<Int256>    { static constexpr const TypeIndex value = TypeIndex::Int256; };
template <> struct TypeId<Float32>  { static constexpr const TypeIndex value = TypeIndex::Float32;  };
template <> struct TypeId<Float64>  { static constexpr const TypeIndex value = TypeIndex::Float64;  };

}

#pragma once

/// Remove the population of thread_local from Poco
#ifdef thread_local
#   undef thread_local
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
    #include <boost/multiprecision/cpp_int.hpp>
#pragma GCC diagnostic pop
#include <ext/singleton.h>
#include <Common/Exception.h>

namespace DB {

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW_ERROR;
}

using int256_t = boost::multiprecision::int256_t;
using int512_t = boost::multiprecision::int512_t;

constexpr int decimal_max_prec = 65;
constexpr int decimal_max_scale = 30;

using PrecType = uint16_t;
using ScaleType = uint8_t;

template<typename T> struct IntPrec{};
template<> struct IntPrec<int8_t>{
    static const PrecType prec = 3;
};
template<> struct IntPrec<uint8_t>{
    static const PrecType prec = 3;
};
template<> struct IntPrec<int16_t>{
    static const PrecType prec = 5;
};
template<> struct IntPrec<uint16_t>{
    static const PrecType prec = 5;
};
template<> struct IntPrec<int32_t>{
    static const PrecType prec = 10;
};
template<> struct IntPrec<uint32_t>{
    static const PrecType prec = 10;
};
template<> struct IntPrec<int64_t>{
    static const PrecType prec = 20;
};
template<> struct IntPrec<uint64_t>{
    static const PrecType prec = 20;
};

//  1) If the declared type of both operands of a dyadic arithmetic operator is exact numeric, then the declared
//  type of the result is an implementation-defined exact numeric type, with precision and scale determined as
//  follows:
//    a) Let S1 and S2 be the scale of the first and second operands respectively.
//    b) The precision of the result of addition and subtraction is implementation-defined, and the scale is the
//       maximum of S1 and S2.
//    c) The precision of the result of multiplication is implementation-defined, and the scale is S1 + S2.
//    d) The precision and scale of the result of division are implementation-defined.

struct InfererHelper {
    template<typename T>
    static T min(T A, T B) {
        return A < B ? A: B;
    }

    template<typename T>
    static T max(T A, T B) {
        return A > B ? A: B;
    }
};

struct PlusDecimalInferer : public InfererHelper {
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_scale = max(left_scale, right_scale);
        PrecType result_int = max(left_prec - left_scale, right_prec - right_scale);
        result_prec = min(result_scale + result_int + 1, decimal_max_prec);
    }
};

struct MulDecimalInferer : public InfererHelper {
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_scale = min(left_scale + right_scale, decimal_max_scale);
        result_prec = min(left_prec + right_prec, decimal_max_prec);
    }
};

struct DivDecimalInferer : public InfererHelper {
    static const uint8_t div_precincrement = 4;
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType /* right_prec is not used */ , ScaleType right_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_prec = min(left_prec + right_scale + div_precincrement, decimal_max_prec);
        result_scale = min(left_scale + div_precincrement, decimal_max_scale);
    }
};

struct SumDecimalInferer : public InfererHelper {
    static constexpr uint8_t decimal_longlong_digits = 22;
    static inline void infer(PrecType prec, ScaleType scale, PrecType &result_prec, ScaleType &result_scale) {
        result_prec = min(prec + decimal_longlong_digits, decimal_max_prec);
        result_scale = scale;
    }
};

struct AvgDecimalInferer : public InfererHelper {
    static const uint8_t div_precincrement = 4;
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_prec = min(left_prec + div_precincrement, decimal_max_prec);
        result_scale = min(left_scale + div_precincrement, decimal_max_scale);
    }
};

struct OtherInferer {
    static inline void infer(PrecType, ScaleType , PrecType , ScaleType, PrecType&, ScaleType&) {}
};

// TODO use template to make type of value arguable.
struct Decimal {
    int256_t value;
    PrecType precision;
    ScaleType scale;

    Decimal(const Decimal& d): value(d.value), precision(d.precision), scale(d.scale) {}
    Decimal():value(0), precision(0), scale(0) {}
    Decimal(int256_t v_, PrecType prec_, ScaleType scale_): value(v_), precision(prec_), scale(scale_){}

    template<typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr >
    Decimal(T v): value(v), precision(IntPrec<T>::prec), scale(0) {}

    template<typename T, std::enable_if_t<std::is_floating_point<T>{}>* = nullptr >
    Decimal(T) {
        throw Exception("please use cast function to convert float to decimal");
    }

    // check if Decimal is inited without any change.
    bool isZero() const {
        return precision == 0 && scale == 0;
    }

    Decimal operator + (const Decimal& v) const ;

    void operator += (const Decimal& v) ;

    void operator = (const Decimal& v) {
        value = v.value;
        precision = v.precision;
        scale = v.scale;
    }

    Decimal operator - (const Decimal& v) const ;

    Decimal operator - () const ;

    Decimal operator ~ () const ;

    Decimal operator * (const Decimal& v) const ;

    template<typename T, std::enable_if_t<std::is_floating_point<T>{}>* = nullptr>
    T operator*(const T& v) const
    {
        return static_cast<T>(*this) * (v);
    }

    template<typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr>
    Decimal operator*(const T& v) const
    {
        return (*this) * static_cast<Decimal>(v);
    }

    Decimal operator / (const Decimal& v) const ;

    template <typename T, std::enable_if_t<std::is_floating_point<T>{}>* = nullptr>
    operator T () const {
        T result = static_cast<T> (value);
        for (ScaleType i = 0; i < scale; i++) {
            result /= 10;
        }
        return result;
    }

    template <typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr>
    operator T () const {
        int256_t v = value;
        for (ScaleType i = 0; i < scale; i++) {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }
        if (v > std::numeric_limits<T>::max() || v < std::numeric_limits<T>::min()) {
            throw Exception("Decimal value overflow", ErrorCodes::DECIMAL_OVERFLOW_ERROR);
        }
        T result = static_cast<T>(v);
        return result;
    }

    bool operator < (const Decimal& v) const;

    bool operator <= (const Decimal& v) const;

    bool operator == (const Decimal& v) const;

    bool operator >= (const Decimal& v) const;

    bool operator > (const Decimal& v) const;

    bool operator != (const Decimal& v) const;

    void checkOverflow() const;

    std::string toString() const;

    PrecType getRealPrec() const;

    void ScaleTo(PrecType prec_, ScaleType scale_) {
        if (scale_ > scale) {
            for(ScaleType i = 0 ; i < scale_ - scale; i++) {
                value *= 10;
            }
        } else {
            bool need2Round = false;
            for(ScaleType i = 0 ; i < scale - scale_; i++) {
                need2Round = (value < 0 ? - value : value) % 10 >= 5;
                value /= 10;
            }
            if (need2Round) {
                if (value < 0)
                    value --;
                else 
                    value ++;
            }
        }
        if (getRealPrec() > prec_) {
            throw Exception("Decimal value overflow", ErrorCodes::DECIMAL_OVERFLOW_ERROR);
        }
        precision = prec_;
        scale = scale_;
    }

    Decimal getAvg(uint64_t cnt, PrecType result_prec, ScaleType result_scale) const {
        auto tmpValue = value;
        if (result_scale > scale) {
            for (ScaleType i = 0; i < result_scale - scale; i++) {
                tmpValue *= 10;
            }
        }
        tmpValue /= cnt;
        Decimal dec(tmpValue, result_prec, result_scale);
        dec.checkOverflow();
        return dec;
    }
};

template <typename DataType> constexpr bool IsDecimal = false;
template <> constexpr bool IsDecimal<Decimal> = true;

bool parseDecimal(const char *str, size_t len, Decimal& dec);

class DecimalMaxValue final : public ext::singleton<DecimalMaxValue> {
    friend class ext::singleton<DecimalMaxValue>;

    int256_t number[decimal_max_prec+1];

public:
    DecimalMaxValue() {
        for (int i = 1; i <= decimal_max_prec; i++) {
            number[i] = number[i-1] * 10 + 9;
        }
    }

    int256_t get(PrecType idx) const {
        return number[idx];
    }
    
    static int256_t Get(PrecType idx) {
        return instance().get(idx);
    }
} ;

template<typename T>
std::enable_if_t<std::is_integral_v<T>, Decimal> ToDecimal(T value, PrecType prec, ScaleType scale)
{
    Decimal dec(value);
    dec.ScaleTo(prec, scale);
    return dec;
}

template<typename T>
std::enable_if_t<std::is_floating_point_v<T>, Decimal> ToDecimal(T value, PrecType prec, ScaleType scale)
{
    bool neg = false;
    if (value < 0) {
        neg = true;
        value = -value;
    }
    for (ScaleType i = 0; i < scale; i++)
    {
        value *= 10;
    }
    if (std::abs(value) > static_cast<T>(DecimalMaxValue::Get(decimal_max_prec)))
    {
        throw Exception("Decimal value overflow", ErrorCodes::DECIMAL_OVERFLOW_ERROR);
    }
    // rounding
    T tenTimesValue = value * 10;
    int256_t v(value);
    if (int256_t(tenTimesValue) % 10 >= 5) {
        v++;
    }
    if (neg) {
        v = -v;
    }
    Decimal dec(v, prec, scale);
    dec.checkOverflow();
    return dec;
}

Decimal ToDecimal(Decimal dec, PrecType prec, ScaleType scale);

}

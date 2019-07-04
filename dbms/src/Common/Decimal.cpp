#include <Common/Decimal.h>
#include <Core/Field.h>

namespace DB {

template<typename T>
String Decimal<T>::toString(ScaleType scale) const 
{
    PrecType precision = maxDecimalPrecision<Decimal<T>>();
    char str[decimal_max_prec + 5];
    size_t len = precision;
    if (value < 0) { // extra space for sign
        len ++;
    }
    if (scale > 0) { // for factional point
        len ++;
    }
    if (scale == precision) { // for leading zero
        len ++;
    }
    size_t end_point = len;
    Int256 cur_v = value;
    if (value < 0) {
        cur_v = -cur_v;
    }
    if (scale > 0) {
        for (size_t i = 0; i < scale; i++)
        {
            int d = static_cast<int>(cur_v % 10);
            cur_v = cur_v / 10;
            str[--len] = d + '0';
        }
        str[--len] = '.';
    }
    do {
        int d = static_cast<int>(cur_v % 10);
        cur_v = cur_v / 10;
        str[--len] = d + '0';
    } while(cur_v > 0);
    if (value < 0) {
        str[--len] = '-';
    }
    return std::string(str + len, end_point - len);
}

bool parseDecimal(const char* str, size_t len, bool negative, Field & field) {
    PrecType prec = 0;
    ScaleType scale = 0;
    Int256 value = 0; // Int256 is ok for 65 digits number at most.
    bool frac = false;
    for (size_t i = 0; i < len; i++) {
        char c = str[i];
        if (c == '.') {
            if (frac || i==0) {
                return false;
            }
            frac = true; 
        } else if (c <= '9' && c >= '0') {
            value = value * 10 + int(c-'0');
            if (frac) scale++;
            if (frac || value > 0) prec ++;
            if (prec > decimal_max_prec || scale > decimal_max_scale)
                return false;
        } else {
            return false;
        }
    }
    if (prec == 0){
        prec ++;
    }

    if (negative) {
        value = -value;
    }

    if (prec <= maxDecimalPrecision<Decimal32>()) {
        field = DecimalField<Decimal32>(static_cast<Int32>(value), scale);
    } else if (prec <= maxDecimalPrecision<Decimal64>()) {
        field = DecimalField<Decimal64>(static_cast<Int64>(value), scale);
    } else if (prec <= maxDecimalPrecision<Decimal128>()) {
        field = DecimalField<Decimal128>(static_cast<Int128>(value), scale);
    } else if (prec <= maxDecimalPrecision<Decimal256>()){
        field = DecimalField<Decimal256>(value, scale);
    } else {
        // This branch expect to be dead code. Cause if prec > decimal_max_prec,
        // it will return false in for-loop
        throw Exception("Decimal Overflow");
    }

    return true;
}

template struct Decimal<Int32>;
template struct Decimal<Int64>;
template struct Decimal<Int128>;
template struct Decimal<Int256>;

// end namespace
}

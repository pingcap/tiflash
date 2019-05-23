#include <Common/Decimal.h>
#include <Core/Field.h>

namespace DB {

//PrecType Decimal::getRealPrec() const {
//    auto _v = value < 0 ? - value : value;
//    for (PrecType i = 1; i <= decimal_max_prec; i++) 
//    {
//        if (DecimalMaxValue::Get(i) >= _v)
//        {
//            return i;
//        }
//    }
//    throw Exception("Decimal value overflow", ErrorCodes::DECIMAL_OVERFLOW_ERROR);
//}
//
//Decimal Decimal::operator + (const Decimal & v) const {
//    ScaleType result_scale;
//    PrecType result_prec;
//    PlusDecimalInferer::infer(precision, scale, v.precision, v.scale, result_prec, result_scale);
//    int256_t value_a = value, value_b = v.value;
//    for (ScaleType s = scale; s < result_scale; s++){
//        value_a *= 10;
//    }
//    for (ScaleType s = v.scale; s < result_scale; s++){
//        value_b *= 10;
//    }
//    int256_t result_value = value_a + value_b;
//    checkOverFlow(result_value, result_prec);
//    return Decimal(result_value, result_prec, result_scale);
//}
//
//void Decimal::operator += (const Decimal & v) {
//    if (precision == 0) {
//        *this = v;
//    } 
//    else if (scale == v.scale)
//    {
//        value = value + v.value;
//        checkOverFlow(value, precision);
//    } else {
//        *this = *this + v;
//    }
//}
//
//Decimal Decimal::operator - (const Decimal & v) const {
//    Decimal tmp = v;
//    tmp.value = -tmp.value;
//    return (*this) + tmp;
//}
//
//Decimal Decimal::operator - () const {
//    return Decimal(-value, precision, scale);
//}
//
//Decimal Decimal::operator ~ () const {
//    return Decimal(~value, precision, scale);
//}
//
//Decimal Decimal::operator * (const Decimal & v) const {
//    ScaleType result_scale;
//    PrecType result_prec;
//    MulDecimalInferer::infer(precision, scale, v.precision, v.scale, result_prec, result_scale);
//    int256_t result_value = value * v.value;
//    ScaleType trunc = scale + v.scale - result_scale;
//    while (trunc > 0) {
//        trunc --;
//        result_value /= 10;
//    }
//    checkOverFlow(result_value, result_prec);
//    return Decimal(result_value, result_prec, result_scale);
//}
//
//Decimal Decimal::operator / (const Decimal & v) const {
//    ScaleType result_scale;
//    PrecType result_prec;
//    DivDecimalInferer::infer(precision, scale, v.precision, v.scale, result_prec, result_scale);
//    int256_t result_value = value;
//    for (ScaleType i = 0; i < v.scale + (result_scale - scale); i++)
//        result_value *= 10;
//    result_value /= v.value;
//    checkOverFlow(result_value, result_prec);
//    return Decimal(result_value, result_prec, result_scale);
//}
//
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

//enum cmpResult {
//    gt = 0,
//    eq = 1,
//    ls = 2,
//};
//
//inline cmpResult scaleAndCompare(const Decimal & v1, const Decimal & v2) {
//    int256_t nv = v1.value;
//    for (ScaleType i = v1.scale; i < v2.scale; i++) {
//        nv = nv * 10;
//    }
//    return nv < v2.value ? cmpResult::ls : ( nv == v2.value? cmpResult::eq : cmpResult::gt );
//}
//
//bool Decimal::operator == (const Decimal & v) const {
//    if (scale == v.scale) {
//        return value == v.value;
//    } else if (scale < v.scale) {
//        cmpResult comp = scaleAndCompare(*this, v);
//        return comp == cmpResult::eq;
//    } else {
//        cmpResult comp = scaleAndCompare(v, *this);
//        return comp == cmpResult::eq;
//    }
//}
//
//bool Decimal::operator < (const Decimal & v) const {
//    if (scale == v.scale) {
//        return value < v.value;
//    } else if (scale < v.scale) {
//        cmpResult comp = scaleAndCompare(*this, v);
//        return comp == cmpResult::ls;
//    } else {
//        cmpResult comp = scaleAndCompare(v, *this);
//        return comp == cmpResult::gt;
//    }
//}
//
//bool Decimal::operator != (const Decimal & v) const {
//    return !(*this == v);
//}
//
//bool Decimal::operator >= (const Decimal & v) const {
//    return !(*this < v);
//}
//
//bool Decimal::operator <= (const Decimal & v) const {
//    return !(*this > v);
//}
//
//bool Decimal::operator > (const Decimal & v) const {
//    return v < *this;
//}

bool parseDecimal(const char* str, size_t len, bool negative, Field & field) {
    PrecType prec = 0;
    ScaleType scale = 0;
    Int256 value = 0;
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
        throw Exception("Decimal Overflow");
    }

    return true;
}

template class Decimal<Int32>;
template class Decimal<Int64>;
template class Decimal<Int128>;
template class Decimal<Int256>;

//Decimal ToDecimal(Decimal dec, PrecType prec, ScaleType scale) {
//    dec.ScaleTo(prec, scale);
//    return dec;
//}

// end namespace
}

#pragma once

#include <boost/endian/conversion.hpp>

namespace DB
{

template <typename T>
inline T toLittleEndian(const T & x)
{
    if constexpr (boost::endian::order::native == boost::endian::order::little)
        return x;
    else
        return boost::endian::endian_reverse(x);
}

template <typename T>
inline T toBigEndian(const T & x)
{
    if constexpr (boost::endian::order::native == boost::endian::order::little)
        return boost::endian::endian_reverse(x);
    else
        return x;
}

template <typename T>
inline T readLittleEndian(const char * addr)
{
    return toLittleEndian(*(reinterpret_cast<const T *>(addr)));
}

template <typename T>
inline T readBigEndian(const char * addr)
{
    return toBigEndian(*(reinterpret_cast<const T *>(addr)));
}

} // namespace DB
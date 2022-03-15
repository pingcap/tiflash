// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

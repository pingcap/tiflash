// Copyright 2023 PingCAP, Inc.
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

#include <Common/BitpackingPrimitives.h>
#include <Common/Exception.h>
#include <bitpackinghelpers.h>
#include <common/unaligned.h>

namespace DB
{

template <typename T, bool ASSUME_INPUT_ALIGNED>
void BitpackingPrimitives::packBuffer(unsigned char * dst, const T * src, size_t count, UInt8 width)
{
    if constexpr (ASSUME_INPUT_ALIGNED)
    {
        for (size_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE)
        {
            packGroup(dst + (i * width) / 8, src + i, width);
        }
    }
    else
    {
        size_t misaligned_count = count % BITPACKING_ALGORITHM_GROUP_SIZE;
        T tmp_buffer[BITPACKING_ALGORITHM_GROUP_SIZE]; // TODO: maybe faster on the heap?
        count -= misaligned_count;
        for (size_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE)
        {
            packGroup<T>(dst + (i * width) / 8, src + i, width);
        }
        // Input was not aligned to BITPACKING_ALGORITHM_GROUP_SIZE, we need a copy
        if (misaligned_count)
        {
            memcpy(tmp_buffer, src + count, misaligned_count * sizeof(T));
            packGroup<T>(dst + (count * width) / 8, tmp_buffer, width);
        }
    }
}

template <typename T>
void BitpackingPrimitives::unPackBuffer(
    unsigned char * dst,
    const unsigned char * src,
    size_t count,
    UInt8 width,
    bool skip_sign_extension)
{
    for (size_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE)
    {
        unPackGroup<T>(dst + i * sizeof(T), src + (i * width) / 8, width, skip_sign_extension);
    }
}

template <typename T>
void BitpackingPrimitives::packBlock(unsigned char * dst, const T * src, UInt8 width)
{
    return packGroup<T>(dst, src, width);
}

template <typename T>
void BitpackingPrimitives::unPackBlock(
    unsigned char * dst,
    const unsigned char * src,
    UInt8 width,
    bool skip_sign_extension)
{
    return unPackGroup<T>(dst, src, width, skip_sign_extension);
}

template <typename T, bool is_signed>
UInt8 BitpackingPrimitives::minimumBitWidth(T value)
{
    return findMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(value, value);
}

template <typename T, bool is_signed>
UInt8 BitpackingPrimitives::minimumBitWidth(const T * values, size_t count)
{
    return findMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(values, count);
}

template <typename T, bool is_signed>
UInt8 BitpackingPrimitives::minimumBitWidth(T minimum, T maximum)
{
    return findMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(minimum, maximum);
}

size_t BitpackingPrimitives::getRequiredSize(size_t count, UInt8 width)
{
    count = roundUpToAlgorithmGroupSize(count);
    return ((count * width) / 8);
}

template <typename T>
T BitpackingPrimitives::roundUpToAlgorithmGroupSize(T num_to_round)
{
    int remainder = num_to_round % BITPACKING_ALGORITHM_GROUP_SIZE;
    if (remainder == 0)
    {
        return num_to_round;
    }

    return num_to_round + BITPACKING_ALGORITHM_GROUP_SIZE - remainder;
}

template <typename T, bool is_signed, bool round_to_next_byte>
UInt8 BitpackingPrimitives::findMinimumBitWidth(const T * values, size_t count)
{
    T min_value = values[0];
    T max_value = values[0];

    for (size_t i = 1; i < count; i++)
    {
        if (values[i] > max_value)
        {
            max_value = values[i];
        }

        if (is_signed)
        {
            if (values[i] < min_value)
            {
                min_value = values[i];
            }
        }
    }

    return findMinimumBitWidth<T, round_to_next_byte>(min_value, max_value);
}

template <typename T, bool is_signed, bool round_to_next_byte>
UInt8 BitpackingPrimitives::findMinimumBitWidth(T min_value, T max_value)
{
    UInt8 bitwidth;
    T value;

    if constexpr (is_signed)
    {
        if (min_value == std::numeric_limits<T>::Minimum())
        {
            // handle special case of the minimal value, as it cannot be negated like all other values.
            return sizeof(T) * 8;
        }
        else
        {
            value = MaxValue(static_cast<T>(-min_value), max_value);
        }
    }
    else
    {
        value = max_value;
    }

    if (value == 0)
    {
        return 0;
    }

    if constexpr (is_signed)
    {
        bitwidth = 1;
    }
    else
    {
        bitwidth = 0;
    }

    while (value)
    {
        bitwidth++;
        value >>= 1;
    }

    bitwidth = getEffectiveWidth<T>(bitwidth);

    // Assert results are correct
#ifndef NDEBUG
    if (bitwidth < sizeof(T) * 8 && bitwidth != 0)
    {
        if constexpr (is_signed)
        {
            RUNTIME_ASSERT(max_value <= (T(1) << (bitwidth - 1)) - 1);
            RUNTIME_ASSERT(min_value >= (T(-1) * ((T(1) << (bitwidth - 1)) - 1) - 1));
        }
        else
        {
            RUNTIME_ASSERT(max_value <= (T(1) << (bitwidth)) - 1);
        }
    }
#endif
    if constexpr (round_to_next_byte)
    {
        return (bitwidth / 8 + (bitwidth % 8 != 0)) * 8;
    }
    return bitwidth;
}

template <typename T, typename TU>
void BitpackingPrimitives::signExtend(unsigned char * dst, UInt8 width)
{
    T const mask = static_cast<T>(TU(1) << (width - 1));
    for (size_t i = 0; i < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE; ++i)
    {
        T value = unalignedLoad<T>(dst + i * sizeof(T));
        value = static_cast<T>(value & ((TU(1) << width) - TU(1)));
        T result = (value ^ mask) - mask;
        Store(result, dst + i * sizeof(T));
    }
}

template <typename T>
UInt8 BitpackingPrimitives::getEffectiveWidth(UInt8 width)
{
    UInt8 bits_of_type = sizeof(T) * 8;
    UInt8 type_size = sizeof(T);
    if (width + type_size > bits_of_type)
    {
        return bits_of_type;
    }
    return width;
}

template <typename T>
void BitpackingPrimitives::packGroup(unsigned char * dst, const T * values, UInt8 width)
{
    if constexpr (std::is_same<T, Int8>::value || std::is_same<T, UInt8>::value)
    {
        fastpforlib::fastpack(
            reinterpret_cast<const UInt8 *>(values),
            reinterpret_cast<UInt8 *>(dst),
            static_cast<UInt32>(width));
    }
    else if constexpr (std::is_same<T, Int16>::value || std::is_same<T, UInt16>::value)
    {
        fastpforlib::fastpack(
            reinterpret_cast<const UInt16 *>(values),
            reinterpret_cast<UInt16 *>(dst),
            static_cast<UInt32>(width));
    }
    else if constexpr (std::is_same<T, Int32>::value || std::is_same<T, UInt32>::value)
    {
        fastpforlib::fastpack(
            reinterpret_cast<const UInt32 *>(values),
            reinterpret_cast<UInt32 *>(dst),
            static_cast<UInt32>(width));
    }
    else if constexpr (std::is_same<T, Int64>::value || std::is_same<T, UInt64>::value)
    {
        fastpforlib::fastpack(
            reinterpret_cast<const UInt64 *>(values),
            reinterpret_cast<UInt32 *>(dst),
            static_cast<UInt32>(width));
    }
    else
    {
        throw Exception("Unsupported type for bitpacking");
    }
}

template <typename T>
void BitpackingPrimitives::unPackGroup(
    unsigned char * dst,
    const unsigned char * src,
    UInt8 width,
    bool skip_sign_extension)
{
    if constexpr (std::is_same<T, Int8>::value || std::is_same<T, UInt8>::value)
    {
        fastpforlib::fastunpack(
            reinterpret_cast<const UInt8 *>(src),
            reinterpret_cast<UInt8 *>(dst),
            static_cast<UInt32>(width));
    }
    else if constexpr (std::is_same<T, Int16>::value || std::is_same<T, UInt16>::value)
    {
        fastpforlib::fastunpack(
            reinterpret_cast<const UInt16 *>(src),
            reinterpret_cast<UInt16 *>(dst),
            static_cast<UInt32>(width));
    }
    else if constexpr (std::is_same<T, Int32>::value || std::is_same<T, UInt32>::value)
    {
        fastpforlib::fastunpack(
            reinterpret_cast<const UInt32 *>(src),
            reinterpret_cast<UInt32 *>(dst),
            static_cast<UInt32>(width));
    }
    else if constexpr (std::is_same<T, Int64>::value || std::is_same<T, UInt64>::value)
    {
        fastpforlib::fastunpack(
            reinterpret_cast<const UInt32 *>(src),
            reinterpret_cast<UInt64 *>(dst),
            static_cast<UInt32>(width));
    }
    else
    {
        throw Exception("Unsupported type for bitpacking");
    }

    if (std::numeric_limits<T>::IsSigned() && !skip_sign_extension && width > 0 && width < sizeof(T) * 8)
    {
        signExtend<T>(dst, width);
    }
}

} // namespace DB

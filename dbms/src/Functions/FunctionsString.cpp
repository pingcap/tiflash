#include <Columns/ColumnArray.h>
#include <Common/UTF8Helpers.h>
#include <Common/Volnitsky.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeArray.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/CharUtil.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/FunctionsRound.h>
#include <Functions/FunctionsString.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/StringUtil.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>
#include <fmt/printf.h>

#include <boost/algorithm/string/predicate.hpp>
#include <ext/range.h>
#include <thread>

#if __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using namespace GatherUtils;

template <bool negative = false>
struct EmptyImpl
{
    /// If the function will return constant value for FixedString data type.
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars_t & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        ColumnString::Offset prev_offset = 1;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i] + 1;
        }
    }

    /// Only make sense if is_fixed_to_constant.
    static void vector_fixed_to_constant(const ColumnString::Chars_t & /*data*/, size_t /*n*/, UInt8 & /*res*/)
    {
        throw Exception("Logical error: 'vector_fixed_to_constant method' is called", ErrorCodes::LOGICAL_ERROR);
    }

    static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt8> & res)
    {
        std::vector<char> empty_chars(n);
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            res[i] = negative ^ (0 == memcmp(&data[i * size], empty_chars.data(), n));
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i];
        }
    }
};


/** Calculates the length of a string in bytes.
  */
struct LengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars_t & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = i == 0 ? (offsets[i] - 1) : (offsets[i] - 1 - offsets[i - 1]);
    }

    static void vector_fixed_to_constant(const ColumnString::Chars_t & /*data*/, size_t n, UInt64 & res)
    {
        res = n;
    }

    static void vector_fixed_to_vector(const ColumnString::Chars_t & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = i == 0 ? (offsets[i]) : (offsets[i] - offsets[i - 1]);
    }
};


/** If the string is UTF-8 encoded text, it returns the length of the text in code points.
  * (not in characters: the length of the text "ё" can be either 1 or 2, depending on the normalization)
 * (not in characters: the length of the text "" can be either 1 or 2, depending on the normalization)
  * Otherwise, the behavior is undefined.
  */
struct LengthUTF8Impl
{
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed_to_constant(const ColumnString::Chars_t & /*data*/, size_t /*n*/, UInt64 & /*res*/)
    {
    }

    static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[i * n], n);
        }
    }

    static void array(const ColumnString::Offsets &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vector_fixed(const ColumnString::Chars_t & data, size_t /*n*/, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        const auto flip_case_mask = 'A' ^ 'a';

#if __SSE2__
        const auto bytes_sse = sizeof(__m128i);
        const auto src_end_sse = src_end - (src_end - src) % bytes_sse;

        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse)
        {
            /// load 16 sequential 8-bit characters
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

            /// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
            const auto is_not_case
                = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));

            /// keep `flip_case_mask` only where necessary, zero out elsewhere
            const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

            /// flip case by applying calculated mask
            const auto cased_chars = _mm_xor_si128(chars, xor_mask);

            /// store result back to destination
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
        }
#endif

        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
    }
};

/** Expands the string in bytes.
  */
struct ReverseImpl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
                res_data[j] = data[offsets[i] + prev_offset - 2 - j];
            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                res_data[j] = data[(i * 2 + 1) * n - j - 1];
    }
};


/** Expands the sequence of code points in a UTF-8 encoded string.
  * The result may not match the expected result, because modifying code points (for example, diacritics) may be applied to another symbols.
  * If the string is not encoded in UTF-8, then the behavior is undefined.
  */
struct ReverseUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            while (j < offsets[i] - 1)
            {
                if (data[j] < 0xBF)
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
                else if (data[j] < 0xE0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 1], &data[j], 2);
                    j += 2;
                }
                else if (data[j] < 0xF0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 2], &data[j], 3);
                    j += 3;
                }
                else
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
            }

            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed(const ColumnString::Chars_t &, size_t, ColumnString::Chars_t &)
    {
        throw Exception("Cannot apply function reverseUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector(
    const ColumnString::Chars_t & data,
    const IColumn::Offsets & offsets,
    ColumnString::Chars_t & res_data,
    IColumn::Offsets & res_offsets)
{
    res_data.resize(data.size());
    res_offsets.assign(offsets);
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector_fixed(
    const ColumnString::Chars_t & data,
    size_t /*n*/,
    ColumnString::Chars_t & res_data)
{
    res_data.resize(data.size());
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::constant(
    const std::string & data,
    std::string & res_data)
{
    res_data.resize(data.size());
    array(reinterpret_cast<const UInt8 *>(data.data()),
          reinterpret_cast<const UInt8 *>(data.data() + data.size()),
          reinterpret_cast<UInt8 *>(&res_data[0]));
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(
    const UInt8 *& src,
    const UInt8 * src_end,
    UInt8 *& dst)
{
    if (src[0] <= ascii_upper_bound)
    {
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst++ = *src++ ^ flip_case_mask;
        else
            *dst++ = *src++;
    }
    else if (src + 1 < src_end
             && ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) || (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
    {
        cyrillic_to_case(src, dst);
    }
    else if (src + 1 < src_end && src[0] == 0xC2u)
    {
        /// Punctuation U+0080 - U+00BF, UTF-8: C2 80 - C2 BF
        *dst++ = *src++;
        *dst++ = *src++;
    }
    else if (src + 2 < src_end && src[0] == 0xE2u)
    {
        /// Characters U+2000 - U+2FFF, UTF-8: E2 80 80 - E2 BF BF
        *dst++ = *src++;
        *dst++ = *src++;
        *dst++ = *src++;
    }
    else
    {
        static const Poco::UTF8Encoding utf8;

        if (const auto chars = utf8.convert(to_case(utf8.convert(src)), dst, src_end - src))
            src += chars, dst += chars;
        else
            ++src, ++dst;
    }
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::array(
    const UInt8 * src,
    const UInt8 * src_end,
    UInt8 * dst)
{
#if __SSE2__
    const auto bytes_sse = sizeof(__m128i);
    auto src_end_sse = src + (src_end - src) / bytes_sse * bytes_sse;

    /// SSE2 packed comparison operate on signed types, hence compare (c < 0) instead of (c > 0x7f)
    const auto v_zero = _mm_setzero_si128();
    const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
    const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
    const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

    while (src < src_end_sse)
    {
        const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

        /// check for ASCII
        const auto is_not_ascii = _mm_cmplt_epi8(chars, v_zero);
        const auto mask_is_not_ascii = _mm_movemask_epi8(is_not_ascii);

        /// ASCII
        if (mask_is_not_ascii == 0)
        {
            const auto is_not_case
                = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));
            const auto mask_is_not_case = _mm_movemask_epi8(is_not_case);

            /// everything in correct case ASCII
            if (mask_is_not_case == 0)
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), chars);
            else
            {
                /// ASCII in mixed case
                /// keep `flip_case_mask` only where necessary, zero out elsewhere
                const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

                /// flip case by applying calculated mask
                const auto cased_chars = _mm_xor_si128(chars, xor_mask);

                /// store result back to destination
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
            }

            src += bytes_sse, dst += bytes_sse;
        }
        else
        {
            /// UTF-8
            const auto expected_end = src + bytes_sse;

            while (src < expected_end)
                toCase(src, src_end, dst);

            /// adjust src_end_sse by pushing it forward or backward
            const auto diff = src - expected_end;
            if (diff != 0)
            {
                if (src_end_sse + diff < src_end)
                    src_end_sse += diff;
                else
                    src_end_sse -= bytes_sse - diff;
            }
        }
    }
#endif
    /// handle remaining symbols
    while (src < src_end)
        toCase(src, src_end, dst);
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::vector(
    const ColumnString::Chars_t & data,
    const IColumn::Offsets & offsets,
    ColumnString::Chars_t & res_data,
    IColumn::Offsets & res_offsets)
{
    res_data.resize(data.size());
    res_offsets.assign(offsets);
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::vector_fixed(
    const ColumnString::Chars_t & data,
    size_t /*n*/,
    ColumnString::Chars_t & res_data)
{
    res_data.resize(data.size());
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::constant(
    const std::string & data,
    std::string & res_data)
{
    res_data.resize(data.size());
    array(reinterpret_cast<const UInt8 *>(data.data()),
          reinterpret_cast<const UInt8 *>(data.data() + data.size()),
          reinterpret_cast<UInt8 *>(&res_data[0]));
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::toCase(
    const UInt8 *& src,
    const UInt8 * src_end,
    UInt8 *& dst)
{
    if (src[0] <= ascii_upper_bound)
    {
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst++ = *src++ ^ flip_case_mask;
        else
            *dst++ = *src++;
    }
    else
    {
        static const Poco::UTF8Encoding utf8;

        if (const auto chars = utf8.convert(to_case(utf8.convert(src)), dst, src_end - src))
            src += chars, dst += chars;
        else
            ++src, ++dst;
    }
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::array(
    const UInt8 * src,
    const UInt8 * src_end,
    UInt8 * dst)
{
#if __SSE2__
    const auto bytes_sse = sizeof(__m128i);
    auto src_end_sse = src + (src_end - src) / bytes_sse * bytes_sse;

    /// SSE2 packed comparison operate on signed types, hence compare (c < 0) instead of (c > 0x7f)
    const auto v_zero = _mm_setzero_si128();
    const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
    const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
    const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

    while (src < src_end_sse)
    {
        const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

        /// check for ASCII
        const auto is_not_ascii = _mm_cmplt_epi8(chars, v_zero);
        const auto mask_is_not_ascii = _mm_movemask_epi8(is_not_ascii);

        /// ASCII
        if (mask_is_not_ascii == 0)
        {
            const auto is_not_case
                = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));
            const auto mask_is_not_case = _mm_movemask_epi8(is_not_case);

            /// everything in correct case ASCII
            if (mask_is_not_case == 0)
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), chars);
            else
            {
                /// ASCII in mixed case
                /// keep `flip_case_mask` only where necessary, zero out elsewhere
                const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

                /// flip case by applying calculated mask
                const auto cased_chars = _mm_xor_si128(chars, xor_mask);

                /// store result back to destination
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
            }

            src += bytes_sse, dst += bytes_sse;
        }
        else
        {
            /// UTF-8
            const auto expected_end = src + bytes_sse;

            while (src < expected_end)
                toCase(src, src_end, dst);

            /// adjust src_end_sse by pushing it forward or backward
            const auto diff = src - expected_end;
            if (diff != 0)
            {
                if (src_end_sse + diff < src_end)
                    src_end_sse += diff;
                else
                    src_end_sse -= bytes_sse - diff;
            }
        }
    }
#endif
    /// handle remaining symbols
    while (src < src_end)
        toCase(src, src_end, dst);
}

/** If the string is encoded in UTF-8, then it selects a substring of code points in it.
  * Otherwise, the behavior is undefined.
  */
struct SubstringUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       Int64 original_start,
                       size_t length,
                       bool implicit_length,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            ColumnString::Offset pos = 1;
            ColumnString::Offset bytes_start = 0;
            ColumnString::Offset bytes_length = 0;
            size_t start = 0;
            if (original_start >= 0)
                start = original_start;
            else
            {
                // set the start as string_length - abs(original_start) + 1
                std::vector<ColumnString::Offset> start_offsets;
                ColumnString::Offset current = prev_offset;
                while (current < offsets[i] - 1)
                {
                    start_offsets.push_back(current);
                    if (data[current] < 0xBF)
                        current += 1;
                    else if (data[current] < 0xE0)
                        current += 2;
                    else if (data[current] < 0xF0)
                        current += 3;
                    else
                        current += 1;
                }
                if (static_cast<size_t>(-original_start) > start_offsets.size())
                {
                    // return empty string
                    res_data.resize(res_data.size() + 1);
                    res_data[res_offset] = 0;
                    res_offset++;
                    res_offsets[i] = res_offset;
                    continue;
                }
                start = start_offsets.size() + original_start + 1;
                pos = start;
                j = start_offsets[start - 1];
            }
            while (j < offsets[i] - 1)
            {
                if (pos == start)
                    bytes_start = j - prev_offset + 1;

                if (data[j] < 0xBF)
                    j += 1;
                else if (data[j] < 0xE0)
                    j += 2;
                else if (data[j] < 0xF0)
                    j += 3;
                else
                    j += 1;

                if (implicit_length)
                {
                    // implicit_length means get the substring from start to the end of the string
                    bytes_length = j - prev_offset + 1 - bytes_start;
                }
                else
                {
                    if (pos >= start && pos < start + length)
                        bytes_length = j - prev_offset + 1 - bytes_start;
                    else if (pos >= start + length)
                        break;
                }

                ++pos;
            }

            if (bytes_start == 0)
            {
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = 0;
                ++res_offset;
            }
            else
            {
                size_t bytes_to_copy = std::min(offsets[i] - prev_offset - bytes_start, bytes_length);
                res_data.resize(res_data.size() + bytes_to_copy + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + bytes_start - 1], bytes_to_copy);
                res_offset += bytes_to_copy + 1;
                res_data[res_offset - 1] = 0;
            }
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};


/** If the string is encoded in UTF-8, then it selects a right of code points in it.
  * Otherwise, the behavior is undefined.
  */
struct RightUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       size_t length,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            std::vector<ColumnString::Offset> start_offsets;
            ColumnString::Offset current = prev_offset;
            // TODO: break this loop in advance
            // NOTE: data[offsets[i] -1] = 0, so ignore it
            while (current < offsets[i] - 1)
            {
                start_offsets.push_back(current);
                if (data[current] < 0xBF)
                    current += 1;
                else if (data[current] < 0xE0)
                    current += 2;
                else if (data[current] < 0xF0)
                    current += 3;
                else
                    current += 1;
            }
            if (start_offsets.size() == 0)
            {
                // null
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = 0;
                ++res_offset;
            }
            else
            {
                // not null
                // if(string_length > length, string_length - length, 0)
                auto start_index = start_offsets.size() > length ? start_offsets.size() - length : 0;
                // copy data from start to end of this string
                size_t bytes_to_copy = offsets[i] - start_offsets[start_index];
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[start_offsets[start_index]], bytes_to_copy);
                res_offset += bytes_to_copy;
            }
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};


template <typename Impl, typename Name, typename ResultType>
class FunctionStringOrArrayToT : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringOrArrayToT>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString()
            && !checkDataType<DataTypeArray>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::vector(col->getChars(), col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (Impl::is_fixed_to_constant)
            {
                ResultType res = 0;
                Impl::vector_fixed_to_constant(col->getChars(), col->getN(), res);

                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(col->size(), toField(res));
            }
            else
            {
                auto col_res = ColumnVector<ResultType>::create();

                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->size());
                Impl::vector_fixed_to_vector(col->getChars(), col->getN(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
            }
        }
        else if (const ColumnArray * col = checkAndGetColumn<ColumnArray>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::array(col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Also works with arrays.
class FunctionReverse : public IFunction
{
public:
    static constexpr auto name = "reverse";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReverse>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const Block &) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString()
            && !checkDataType<DataTypeArray>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col->getN());
            ReverseImpl::vector_fixed(col->getChars(), col->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (checkColumn<ColumnArray>(column.get()))
        {
            DefaultExecutable(std::make_shared<FunctionArrayReverse>()).execute(block, arguments, result);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

extern UInt64 GetJsonLength(std::string_view sv);

class FunctionJsonLength : public IFunction
{
public:
    static constexpr auto name = "jsonLength";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonLength>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();
            typename ColumnUInt64::Container & vec_col_res = col_res->getData();
            {
                const auto & data = col->getChars();
                const auto & offsets = col->getOffsets();
                const size_t size = offsets.size();
                vec_col_res.resize(size);

                ColumnString::Offset prev_offset = 0;
                for (size_t i = 0; i < size; ++i)
                {
                    std::string_view sv(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1);
                    vec_col_res[i] = GetJsonLength(sv);
                    prev_offset = offsets[i];
                }
            }
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
};

template <typename Name, bool is_injective>
class ConcatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    ConcatImpl(const Context & context)
        : context(context)
    {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<ConcatImpl>(context);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const Block &) const override
    {
        return is_injective;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!is_injective && !arguments.empty() && checkDataType<DataTypeArray>(arguments[0].get()))
            return FunctionArrayConcat(context).getReturnTypeImpl(arguments);

        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!arg->isStringOrFixedString())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (!is_injective && !arguments.empty() && checkDataType<DataTypeArray>(block.getByPosition(arguments[0]).type.get()))
            return FunctionArrayConcat(context).executeImpl(block, arguments, result);

        if (arguments.size() == 2)
            executeBinary(block, arguments, result);
        else
            executeNAry(block, arguments, result);
    }

private:
    const Context & context;

    void executeBinary(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto c_res = ColumnString::create();

        if (c0_string && c1_string)
            concat(StringSource(*c0_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_string && c1_const_string)
            concat(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_string)
            concat(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else
        {
            /// Fallback: use generic implementation for not very important cases.
            executeNAry(block, arguments, result);
            return;
        }

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeNAry(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        size_t num_sources = arguments.size();
        StringSources sources(num_sources);

        for (size_t i = 0; i < num_sources; ++i)
            sources[i] = createDynamicStringSource(*block.getByPosition(arguments[i]).column);

        auto c_res = ColumnString::create();
        concat(sources, StringSink(*c_res, block.rows()));
        block.getByPosition(result).column = std::move(c_res);
    }
};

/** TiDB Function CONCAT(str1,str2,...)
  * Returns the string that results from concatenating the arguments. May have one or more arguments.
  * CONCAT() returns NULL if any argument is NULL.
*/
class FunctionTiDBConcat : public IFunction
{
private:
    const Context & context;

    struct NameTiDBConcat
    {
        static constexpr auto name = "tidbConcat";
    };

public:
    static constexpr auto name = NameTiDBConcat::name;
    FunctionTiDBConcat(const Context & context)
        : context(context)
    {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTiDBConcat>(context);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto & arg = arguments[arg_idx].get();
            if (!arg->isStringOrFixedString())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
        {
            const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
            block.getByPosition(result).column = c0->cloneResized(c0->size());
        }
        else
            return ConcatImpl<NameTiDBConcat, false>(context).executeImpl(block, arguments, result);
    }
};

/** TiDB Function CONCAT_WS(separator,str1,str2,...)
  * CONCAT_WS() stands for Concatenate With Separator and is a special form of CONCAT().
  * The first argument is the separator for the rest of the arguments.
  * If the separator is NULL, the result is NULL.
  * CONCAT_WS() does not skip empty strings. However, it does skip any NULL values after the separator argument.
*/
class FunctionTiDBConcatWithSeparator : public IFunction
{
public:
    static constexpr auto name = "tidbConcatWS";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBConcatWithSeparator>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = removeNullable(arguments[arg_idx]).get();
            if (!arg->isStringOrFixedString())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        Block nested_block = createBlockWithNestedColumns(block, arguments, result);
        StringSources sources(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            sources[i] = createDynamicStringSource(*nested_block.getByPosition(arguments[i]).column);

        size_t rows = block.rows();
        auto result_null_map = ColumnUInt8::create(rows);
        auto res = ColumnString::create();
        StringSink sink(*res, rows);

        for (size_t row = 0; row < rows; row++)
        {
            if (block.getByPosition(arguments[0]).column->isNullAt(row))
            {
                result_null_map->getData()[row] = true;
            }
            else
            {
                result_null_map->getData()[row] = false;

                bool has_not_null = false;
                for (size_t col = 1; col < arguments.size(); ++col)
                {
                    if (!block.getByPosition(arguments[col]).column->isNullAt(row))
                    {
                        if (has_not_null)
                            writeSlice(sources[0]->getWhole(), sink);
                        else
                            has_not_null = true;
                        writeSlice(sources[col]->getWhole(), sink);
                    }
                }
            }
            for (size_t col = 0; col < arguments.size(); ++col)
                sources[col]->next();
            sink.next();
        }

        block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(result_null_map));
    }
};

class FunctionSubstring : public IFunction
{
public:
    static constexpr auto name = "substring";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstring>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                                + toString(number_of_arguments) + ", should be 2 or 3",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber())
            throw Exception("Illegal type " + arguments[1]->getName()
                                + " of second argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !arguments[2]->isNumber())
            throw Exception("Illegal type " + arguments[2]->getName()
                                + " of second argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    void executeForSource(
        const ColumnPtr & column_start,
        const ColumnPtr & column_length,
        const ColumnConst * column_start_const,
        const ColumnConst * column_length_const,
        Int64 start_value,
        Int64 length_value,
        Block & block,
        size_t result,
        Source && source) const
    {
        auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_start_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetUnbounded(source, StringSink(*col_res, block.rows()), start_value - 1);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, block.rows()), -start_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetUnbounded(source, StringSink(*col_res, block.rows()), *column_start);
        }
        else
        {
            if (column_start_const && column_length_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetBounded(source, StringSink(*col_res, block.rows()), start_value - 1, length_value);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetBounded(source, StringSink(*col_res, block.rows()), -start_value, length_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetBounded(source, StringSink(*col_res, block.rows()), *column_start, *column_length);
        }

        block.getByPosition(result).column = std::move(col_res);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        ColumnPtr column_length;

        if (number_of_arguments == 3)
            column_length = block.getByPosition(arguments[2]).column;

        const ColumnConst * column_start_const = checkAndGetColumn<ColumnConst>(column_start.get());
        const ColumnConst * column_length_const = nullptr;

        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 start_value = 0;
        Int64 length_value = 0;

        if (column_start_const)
        {
            start_value = column_start_const->getInt(0);
        }
        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception("Third argument provided for function substring could not be negative.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, StringSource(*col));
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, FixedStringSource(*col));
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, ConstSource<StringSource>(*col));
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, ConstSource<FixedStringSource>(*col));
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionSubstringUTF8 : public IFunction
{
public:
    static constexpr auto name = "substringUTF8";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstringUTF8>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t arguments_size = arguments.size();
        if (arguments_size != 2 && arguments_size != 3)
            throw Exception("Function " + getName()
                                + " requires from 2 or 3 parameters: string, start, [length]. Passed "
                                + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber() || (arguments_size == 3 && !arguments[2]->isNumber()))
            throw Exception("Illegal type " + (arguments[1]->isNumber() ? arguments[2]->getName() : arguments[1]->getName())
                                + " of argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;

        const ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        if (!column_start->isColumnConst())
            throw Exception("2nd arguments of function " + getName() + " must be constants.");
        Field start_field = (*block.getByPosition(arguments[1]).column)[0];
        if (start_field.getType() != Field::Types::UInt64 && start_field.getType() != Field::Types::Int64)
            throw Exception("2nd argument of function " + getName() + " must have UInt/Int type.");
        Int64 start;
        if (start_field.getType() == Field::Types::Int64)
        {
            start = start_field.get<Int64>();
        }
        else
        {
            UInt64 u_start = start_field.get<UInt64>();
            if (u_start >= 0x8000000000000000ULL)
                throw Exception("Too large values of 2nd argument provided for function substring.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            start = (Int64)u_start;
        }

        bool implicit_length = true;
        UInt64 length = 0;
        if (arguments.size() == 3)
        {
            implicit_length = false;
            const ColumnPtr column_length = block.getByPosition(arguments[2]).column;
            if (!column_length->isColumnConst())
                throw Exception("3rd arguments of function " + getName() + " must be constants.");
            Field length_field = (*block.getByPosition(arguments[2]).column)[0];
            // tidb will push the 3rd argument as signed int, so have to handle Int64 case
            if (length_field.getType() != Field::Types::UInt64 && length_field.getType() != Field::Types::Int64)
                throw Exception(
                    "3rd argument of function " + getName() + " must have UInt/Int type.");
            if (length_field.getType() == Field::Types::UInt64)
            {
                length = length_field.get<UInt64>();
                /// Otherwise may lead to overflow and pass bounds check inside inner loop.
                if (length >= 0x8000000000000000ULL)
                    throw Exception("Too large values of 3rd argument provided for function substring.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            }
            else
            {
                Int64 signed_length = length_field.get<Int64>();
                // according to mysql doc: "If len is less than 1, the result is the empty string."
                if (signed_length < 0)
                    length = 0;
                else
                    length = signed_length;
            }
        }


        if (start == 0 || (!implicit_length && length == 0))
        {
            block.getByPosition(result).column = DataTypeString().createColumnConst(column_string->size(), toField(String("")));
            return;
        }

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
        {
            auto col_res = ColumnString::create();
            SubstringUTF8Impl::vector(col->getChars(), col->getOffsets(), start, length, implicit_length, col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionRightUTF8 : public IFunction
{
public:
    static constexpr auto name = "rightUTF8";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRightUTF8>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t arguments_size = arguments.size();
        if (arguments_size != 2)
            throw Exception("Function " + getName()
                                + " requires from 2 parameters: string, length. Passed "
                                + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber())
            throw Exception("Illegal type " + arguments[1]->getName()
                                + " of argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;

        const ColumnPtr column_length = block.getByPosition(arguments[1]).column;
        if (!column_length->isColumnConst())
            throw Exception("2nd arguments of function " + getName() + " must be constants.");
        Field length_field = (*block.getByPosition(arguments[1]).column)[0];
        if (length_field.getType() != Field::Types::UInt64 && length_field.getType() != Field::Types::Int64)
            throw Exception("2nd argument of function " + getName() + " must have UInt/Int type.");
        Int64 length;
        if (length_field.getType() == Field::Types::Int64)
        {
            length = length_field.get<Int64>();
        }
        else
        {
            UInt64 u_start = length_field.get<UInt64>();
            if (u_start >= 0x8000000000000000ULL)
                throw Exception("Too large values of 2nd argument provided for function substring.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            length = (Int64)u_start;
        }

        if (length <= 0)
        {
            block.getByPosition(result).column = DataTypeString().createColumnConst(column_string->size(), toField(String("")));
            return;
        }

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
        {
            auto col_res = ColumnString::create();
            RightUTF8Impl::vector(col->getChars(), col->getOffsets(), length, col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionAppendTrailingCharIfAbsent : public IFunction
{
public:
    static constexpr auto name = "appendTrailingCharIfAbsent";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionAppendTrailingCharIfAbsent>();
    }

    String getName() const override
    {
        return name;
    }


private:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!arguments[1]->isString())
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto & column = block.getByPosition(arguments[0]).column;
        const auto & column_char = block.getByPosition(arguments[1]).column;

        if (!checkColumnConst<ColumnString>(column_char.get()))
            throw Exception{"Second argument of function " + getName() + " must be a constant string", ErrorCodes::ILLEGAL_COLUMN};

        String trailing_char_str = static_cast<const ColumnConst &>(*column_char).getValue<String>();

        if (trailing_char_str.size() != 1)
            throw Exception{"Second argument of function " + getName() + " must be a one-character string", ErrorCodes::BAD_ARGUMENTS};

        if (const auto col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            const auto & src_data = col->getChars();
            const auto & src_offsets = col->getOffsets();

            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();

            const auto size = src_offsets.size();
            dst_data.resize(src_data.size() + size);
            dst_offsets.resize(size);

            ColumnString::Offset src_offset{};
            ColumnString::Offset dst_offset{};

            for (const auto i : ext::range(0, size))
            {
                const auto src_length = src_offsets[i] - src_offset;
                memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
                src_offset = src_offsets[i];
                dst_offset += src_length;

                if (src_length > 1 && dst_data[dst_offset - 2] != trailing_char_str.front())
                {
                    dst_data[dst_offset - 1] = trailing_char_str.front();
                    dst_data[dst_offset] = 0;
                    ++dst_offset;
                }

                dst_offsets[i] = dst_offset;
            }

            dst_data.resize_assume_reserved(dst_offset);
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception{
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }
};


template <typename Name, bool ltrim, bool rtrim>
class TrimImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit TrimImpl() {}
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<TrimImpl>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be 1 or 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!arg->isStringOrFixedString())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
            executeTrim(block, arguments, result);
        else if (arguments.size() == 2)
            executeTrimWs(block, arguments, result);
        else
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should beat least 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

private:
    void executeTrim(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);

        auto c_res = ColumnString::create();

        if (c0_string)
            trim<ltrim, rtrim, StringSource, StringSink>(StringSource(*c0_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string)
            trim<ltrim, rtrim, ConstSource<StringSource>, StringSink>(ConstSource<StringSource>(*c0_const_string), StringSink(*c_res, c0->size()));
        else
            throw Exception{"Argument of function " + getName() + " must be string", ErrorCodes::ILLEGAL_COLUMN};

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeTrimWs(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto c_res = ColumnString::create();

        if (c0_string && c1_string)
            trim<ltrim, rtrim, StringSource, StringSource, StringSink>(StringSource(*c0_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_string && c1_const_string)
            trim<ltrim, rtrim, StringSource, ConstSource<StringSource>, StringSink>(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_string)
            trim<ltrim, rtrim, ConstSource<StringSource>, StringSource, StringSink>(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_const_string)
            trim<ltrim, rtrim, ConstSource<StringSource>, ConstSource<StringSource>, StringSink>(ConstSource<StringSource>(*c0_const_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else
            throw Exception{"Argument of function " + getName() + " must be string", ErrorCodes::ILLEGAL_COLUMN};

        block.getByPosition(result).column = std::move(c_res);
    }
};


template <typename Name, bool ltrim, bool rtrim>
class TrimUTF8Impl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit TrimUTF8Impl() {}
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<TrimUTF8Impl>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!arg->isStringOrFixedString())
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function "
                        + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
            executeTrim(block, arguments, result);
        else if (arguments.size() == 2)
            executeTrimWs(block, arguments, result);
        else
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should beat least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

private:
    void executeTrim(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);

        auto c_res = ColumnString::create();

        if (c0_string)
            vector(c0_string->getChars(), c0_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (c0_const_string)
        {
            auto c0_c_string = checkAndGetColumn<ColumnString>(c0_const_string->getDataColumnPtr().get());
            vector(c0_c_string->getChars(), c0_c_string->getOffsets(), c0_const_string->size(), c_res->getChars(), c_res->getOffsets());
        }
        else
            throw Exception{"Argument of function " + getName() + " must be string", ErrorCodes::ILLEGAL_COLUMN};

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeTrimWs(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);
        auto column_trim_string = checkAndGetColumn<ColumnString>(c1_const_string->getDataColumnPtr().get());

        auto c_res = ColumnString::create();

        if (c0_string)
            vectorWS(c0_string->getChars(), c0_string->getOffsets(), column_trim_string->getChars(), column_trim_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (c0_const_string)
        {
            auto c0_c_string = checkAndGetColumn<ColumnString>(c0_const_string->getDataColumnPtr().get());
            vectorWS(c0_c_string->getChars(), c0_c_string->getOffsets(), c0_const_string->size(), column_trim_string->getChars(), column_trim_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        }
        else
            throw Exception{"Argument of function " + getName() + " must be string", ErrorCodes::ILLEGAL_COLUMN};

        block.getByPosition(result).column = std::move(c_res);
    }

    static void vectorWS(const ColumnString::Chars_t & data,
                         const ColumnString::Offsets & offsets,
                         const ColumnString::Chars_t & trim_data,
                         const ColumnString::Offsets & trim_offsets,
                         ColumnString::Chars_t & res_data,
                         ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            ColumnString::Offset trim_len = UTF8::countCodePoints(&trim_data[0], trim_offsets[0] - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset = offsets[i] - 1 - prev_offset;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes, trim_bytes;

                    if (data[prev_offset + per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[prev_offset + per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[prev_offset + per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (bytes == trim_bytes && memcmp(&trim_data[per_trim_offset], &data[prev_offset + per_offset], bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_offset += bytes;
                    }
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    size_t trim_bytes = 0;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (memcmp(&trim_data[per_trim_offset], &data[prev_offset + per_end_offset - trim_bytes], trim_bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_end_offset -= trim_bytes;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[prev_offset + per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            prev_offset = offsets[i];
            res_offsets[i] = res_offset;
        }
    }

    static void vectorWS(const ColumnString::Chars_t & data,
                         const ColumnString::Offsets & offsets,
                         size_t size,
                         const ColumnString::Chars_t & trim_data,
                         const ColumnString::Offsets & trim_offsets,
                         ColumnString::Chars_t & res_data,
                         ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() * size);
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[0], offsets[0] - 1);
            ColumnString::Offset trim_len = UTF8::countCodePoints(&trim_data[0], trim_offsets[0] - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset = offsets[0] - 1;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes, trim_bytes;

                    if (data[per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (bytes == trim_bytes && memcmp(&trim_data[per_trim_offset], &data[per_offset], bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_offset += bytes;
                    }
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    size_t trim_bytes = 0;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (memcmp(&trim_data[per_trim_offset], &data[per_end_offset - trim_bytes], trim_bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_end_offset -= trim_bytes;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
        }
    }

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len;
            len = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset;
            per_end_offset = offsets[i] - 1 - prev_offset;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes;

                    if (data[prev_offset + per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[prev_offset + per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[prev_offset + per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    if (bytes != 1 || memcmp(" ", &data[prev_offset + per_offset], bytes) != 0)
                    {
                        break;
                    }

                    per_offset += bytes;
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    if (memcmp(" ", &data[prev_offset + per_end_offset - 1], 1) != 0)
                    {
                        break;
                    }
                    else
                    {
                        per_end_offset -= 1;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[prev_offset + per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            prev_offset = offsets[i];

            res_offsets[i] = res_offset;
        }
    }

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       size_t size, /// num of rows
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() * size);
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len;
            len = UTF8::countCodePoints(&data[prev_offset], offsets[0] - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset;
            per_end_offset = offsets[0] - 1;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes;

                    if (data[prev_offset + per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[prev_offset + per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[prev_offset + per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    if (bytes != 1 || memcmp(" ", &data[prev_offset + per_offset], bytes) != 0)
                    {
                        break;
                    }

                    per_offset += bytes;
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    if (memcmp(" ", &data[prev_offset + per_end_offset - 1], 1) != 0)
                    {
                        break;
                    }
                    else
                    {
                        per_end_offset -= 1;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[prev_offset + per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            prev_offset = 0;

            res_offsets[i] = res_offset;
        }
    }
};


template <typename Name, bool is_left>
class PadImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit PadImpl() {}
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<PadImpl>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", must be 3.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber())
            throw Exception("Illegal type " + arguments[1]->getName()
                                + " of second argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 3)
            executePad(block, arguments, result);
        else
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should beat least 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

private:
    void executePad(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_length = block.getByPosition(arguments[1]).column;
        ColumnPtr column_padding = block.getByPosition(arguments[2]).column;

        const ColumnConst * column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());
        const ColumnConst * column_padding_const = checkAndGetColumnConst<ColumnString>(column_padding.get());

        Int64 length_value = 0;

        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception("Second argument provided for function " + getName() + " could not be negative.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        if (column_padding_const == nullptr)
        {
            throw Exception("Third argument provided for function " + getName() + " should be literal string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        auto c_res = ColumnString::create();

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            pad<is_left, StringSource, ConstSource<StringSource>, StringSink>(
                StringSource(*col),
                ConstSource<StringSource>(*column_padding_const),
                StringSink(*c_res, col->size()),
                length_value);
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            pad<is_left, FixedStringSource, ConstSource<StringSource>, StringSink>(
                FixedStringSource(*col),
                ConstSource<StringSource>(*column_padding_const),
                StringSink(*c_res, col->size()),
                length_value);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
            pad<is_left, ConstSource<StringSource>, ConstSource<StringSource>, StringSink>(
                ConstSource<StringSource>(*col),
                ConstSource<StringSource>(*column_padding_const),
                StringSink(*c_res, col->size()),
                length_value);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
            pad<is_left, ConstSource<FixedStringSource>, ConstSource<StringSource>, StringSink>(
                ConstSource<FixedStringSource>(*col),
                ConstSource<StringSource>(*column_padding_const),
                StringSink(*c_res, col->size()),
                length_value);

        block.getByPosition(result).column = std::move(c_res);
    }
};

template <typename Name, bool is_left>
class PadUTF8Impl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit PadUTF8Impl() {}
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<PadUTF8Impl>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", must be 3.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber())
            throw Exception("Illegal type " + arguments[1]->getName()
                                + " of second argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 3)
            executePadUTF8(block, arguments, result);
        else
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should beat least 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

private:
    void executePadUTF8(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_length = block.getByPosition(arguments[1]).column;
        ColumnPtr column_padding = block.getByPosition(arguments[2]).column;

        const ColumnConst * column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());
        const ColumnConst * column_padding_const = checkAndGetColumnConst<ColumnString>(column_padding.get());

        Int64 length_value = 0;

        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception("Second argument provided for function " + getName() + " could not be negative.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        if (column_padding_const == nullptr)
        {
            throw Exception("Third argument provided for function " + getName() + " should be literal string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        auto c_res = ColumnString::create();
        auto column_padding_string = checkAndGetColumn<ColumnString>(column_padding_const->getDataColumnPtr().get());
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            vector(col->getChars(), col->getOffsets(), length_value, column_padding_string->getChars(), column_padding_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            vector(col->getChars(), col->getN(), col->size(), length_value, column_padding_string->getChars(), column_padding_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
        {
            const auto * col_string = checkAndGetColumn<ColumnString>(col->getDataColumnPtr().get());
            vector_const(col_string->getChars(),
                         col_string->getOffsets(),
                         col->size(),
                         length_value,
                         column_padding_string->getChars(),
                         column_padding_string->getOffsets(),
                         c_res->getChars(),
                         c_res->getOffsets());
        }
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
        {
            const auto * col_string = checkAndGetColumn<ColumnFixedString>(col->getDataColumnPtr().get());
            vector_const(col_string->getChars(),
                         col_string->getN(),
                         col->size(),
                         length_value,
                         column_padding_string->getChars(),
                         column_padding_string->getOffsets(),
                         c_res->getChars(),
                         c_res->getOffsets());
        }

        block.getByPosition(result).column = std::move(c_res);
    }

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       size_t length,
                       const ColumnString::Chars_t & pad_data,
                       const ColumnString::Offsets & pad_offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_data.reserve(length * 3 * size + size);
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    memcpy(&res_data[res_offset], &data[prev_offset], offsets[i] - prev_offset);
                    res_offset += offsets[i] - prev_offset;
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[prev_offset], offsets[i] - prev_offset - 1);
                    res_offset += offsets[i] - prev_offset - 1;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = prev_offset;

                size_t left = length;
                while (left > 0)
                {
                    /// get length parameter characters
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void vector(const ColumnString::Chars_t & data,
                       size_t fixed_len,
                       size_t size,
                       size_t length,
                       const ColumnString::Chars_t & pad_data,
                       const ColumnString::Offsets & pad_offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(3 * length * size);
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t byte_len = strlen(reinterpret_cast<const char *>(&(data[prev_offset])));
            ColumnString::Offset len = UTF8::countCodePoints(&data[prev_offset], byte_len);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    memcpy(&res_data[res_offset], &data[prev_offset], byte_len);
                    res_offset += byte_len;

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    res_offset += 1;
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[prev_offset], byte_len);
                    res_offset += byte_len;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = prev_offset;

                size_t left = length;

                /// get length parameter characters
                while (left > 0)
                {
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
            prev_offset += fixed_len;
        }
    }

    static void vector_const(const ColumnString::Chars_t & data,
                             const ColumnString::Offsets & offsets,
                             size_t size, /// number of rows of const column
                             size_t length,
                             const ColumnString::Chars_t & pad_data,
                             const ColumnString::Offsets & pad_offsets,
                             ColumnString::Chars_t & res_data,
                             ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(3 * length * size);
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[0], offsets[0] - 1);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    memcpy(&res_data[res_offset], &data[0], offsets[0]);
                    res_offset += offsets[0];
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[0], offsets[0] - 1);
                    res_offset += offsets[0] - 1;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = 0;

                size_t left = length;
                while (left > 0)
                {
                    /// get length parameter characters
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
        }
    }

    static void vector_const(const ColumnString::Chars_t & data,
                             size_t, /// length of fixed colomn
                             size_t size, /// number of row
                             size_t length,
                             const ColumnString::Chars_t & pad_data,
                             const ColumnString::Offsets & pad_offsets,
                             ColumnString::Chars_t & res_data,
                             ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(3 * length * size);
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t byte_len = strlen(reinterpret_cast<const char *>(&(data[0])));
            ColumnString::Offset len = UTF8::countCodePoints(&data[0], byte_len);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    memcpy(&res_data[res_offset], &data[0], byte_len);
                    res_offset += byte_len;

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    res_offset += 1;
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[0], byte_len);
                    res_offset += byte_len;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = 0;

                size_t left = length;

                /// get length parameter characters
                while (left > 0)
                {
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
        }
    }
};

class FunctionASCII : public IFunction
{
public:
    static constexpr auto name = "ascii";
    FunctionASCII(const Context & context)
        : context(context)
    {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionASCII>(context);
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();
        const ColumnConst * c0_const = checkAndGetColumn<ColumnConst>(c0_col);
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0_col);
        const ColumnFixedString * c0_fixed_string = checkAndGetColumn<ColumnFixedString>(c0_col);

        Field res_field;
        int val_num = c0_col->size();
        auto col_res = ColumnInt64::create();
        col_res->reserve(val_num);
        if (c0_const == nullptr && c0_string == nullptr && c0_fixed_string == nullptr)
            throw Exception("Illegal argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (int i = 0; i < val_num; i++)
        {
            c0_col->get(i, res_field);
            String handled_str = res_field.get<String>();
            Int64 res = handled_str.size() == 0 ? 0 : static_cast<Int64>(handled_str[0]);
            col_res->insert(res);
        }

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    const Context & context;
};

class FunctionLength : public IFunction
{
public:
    static constexpr auto name = "length";
    FunctionLength(const Context & context)
        : context(context)
    {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionLength>(context);
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();
        const ColumnConst * c0_const = checkAndGetColumn<ColumnConst>(c0_col);
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0_col);
        const ColumnFixedString * c0_fixed_string = checkAndGetColumn<ColumnFixedString>(c0_col);

        Field res_field;
        int val_num = c0_col->size();
        auto col_res = ColumnInt64::create();
        col_res->reserve(val_num);
        if (c0_const == nullptr && c0_string == nullptr && c0_fixed_string == nullptr)
            throw Exception("Illegal argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (int i = 0; i < val_num; i++)
        {
            c0_col->get(i, res_field);
            String handled_str = res_field.get<String>();
            col_res->insert(static_cast<Int64>(handled_str.size()));
        }

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    const Context & context;
};

class FunctionPosition : public IFunction
{
public:
    static constexpr auto name = "position";
    FunctionPosition(const Context & context)
        : context(context)
    {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionPosition>(context);
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();
        const ColumnConst * c0_const = checkAndGetColumn<ColumnConst>(c0_col);
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0_col);
        const ColumnFixedString * c0_fixed_string = checkAndGetColumn<ColumnFixedString>(c0_col);
        Field c0_field;

        const IColumn * c1_col = block.getByPosition(arguments[1]).column.get();
        const ColumnConst * c1_const = checkAndGetColumn<ColumnConst>(c1_col);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1_col);
        const ColumnFixedString * c1_fixed_string = checkAndGetColumn<ColumnFixedString>(c1_col);
        Field c1_field;

        if ((c0_const == nullptr && c0_string == nullptr && c0_fixed_string == nullptr) || (c1_const == nullptr && c1_string == nullptr && c1_fixed_string == nullptr))
            throw Exception("Illegal argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (c0_col->size() != c1_col->size())
            throw Exception("Function " + getName() + " column number is inconformity", ErrorCodes::LOGICAL_ERROR);

        auto col_res = ColumnInt64::create();
        int val_num = c0_col->size();
        col_res->reserve(val_num);

        for (int i = 0; i < val_num; i++)
        {
            c0_col->get(i, c0_field);
            c1_col->get(i, c1_field);

            String c0_str = c0_field.get<String>();
            String c1_str = c1_field.get<String>();

            // return -1 when c1_str not contains the c0_str
            Int64 idx = c1_str.find(c0_str);
            col_res->insert(getPositionUTF8(c1_str, idx));
        }

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    Int64 getPositionUTF8(const String & c1_str, Int64 idx) const
    {
        if (idx == -1)
            return 0;

        auto data = reinterpret_cast<const UInt8 *>(c1_str.data());
        return static_cast<size_t>(UTF8::countCodePoints(data, idx) + 1);
    }

    const Context & context;
};

class FunctionSubStringIndex : public IFunction
{
public:
    static constexpr auto name = "substringIndex";
    explicit FunctionSubStringIndex(const Context & context_)
        : context(context_)
    {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionSubStringIndex>(context);
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be {}.", getName(), toString(arguments.size()), 3),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[1]->isString())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[2]->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", arguments[2]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (executeSubStringIndex<UInt8>(block, arguments, result)
            || executeSubStringIndex<UInt16>(block, arguments, result)
            || executeSubStringIndex<UInt32>(block, arguments, result)
            || executeSubStringIndex<UInt64>(block, arguments, result)
            || executeSubStringIndex<Int8>(block, arguments, result)
            || executeSubStringIndex<Int16>(block, arguments, result)
            || executeSubStringIndex<Int32>(block, arguments, result)
            || executeSubStringIndex<Int64>(block, arguments, result))
        {
            return;
        }
        else
        {
            throw Exception(fmt::format("Illegal argument of function  {}", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

private:
    const Context & context;

    template <typename IntType>
    bool executeSubStringIndex(
        Block & block,
        const ColumnNumbers & arguments,
        const size_t result) const
    {
        ColumnPtr & column_str = block.getByPosition(arguments[0]).column;
        ColumnPtr & column_delim = block.getByPosition(arguments[1]).column;
        ColumnPtr & column_count = block.getByPosition(arguments[2]).column;
        ColumnWithTypeAndName & column_result = block.getByPosition(result);

        bool delim_const = column_delim->isColumnConst();
        bool count_const = column_count->isColumnConst();

        // TODO: differentiate vector and const
        column_str = column_str->isColumnConst() ? column_str->convertToFullColumnIfConst() : column_str;
        if (delim_const && count_const)
        {
            const ColumnString * str_col = checkAndGetColumn<ColumnString>(column_str.get());
            const ColumnConst * delim_col = checkAndGetColumnConst<ColumnString>(column_delim.get());
            const ColumnConst * count_col = checkAndGetColumnConst<ColumnVector<IntType>>(column_count.get());
            if (str_col == nullptr || delim_col == nullptr || count_col == nullptr)
            {
                return false;
            }
            auto col_res = ColumnString::create();
            IntType count = count_col->getValue<IntType>();
            vectorConstConst(
                str_col->getChars(),
                str_col->getOffsets(),
                delim_col->getValue<String>(),
                accurate::lessOp(INT64_MAX, count) ? INT64_MAX : count,
                col_res->getChars(),
                col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else
        {
            column_delim = column_delim->isColumnConst() ? column_delim->convertToFullColumnIfConst() : column_delim;
            column_count = column_count->isColumnConst() ? column_count->convertToFullColumnIfConst() : column_count;
            const ColumnString * str_col = checkAndGetColumn<ColumnString>(column_str.get());
            const ColumnString * delim_col = checkAndGetColumn<ColumnString>(column_delim.get());
            const ColumnVector<IntType> * count_col = checkAndGetColumn<ColumnVector<IntType>>(column_count.get());
            if (str_col == nullptr || delim_col == nullptr || count_col == nullptr)
            {
                return false;
            }
            auto col_res = ColumnString::create();
            vectorVectorVector(
                str_col->getChars(),
                str_col->getOffsets(),
                delim_col->getChars(),
                delim_col->getOffsets(),
                count_col->getData(),
                col_res->getChars(),
                col_res->getOffsets());
            column_result.column = std::move(col_res);
        }

        return true;
    }

    static void vectorConstConst(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & delim,
        const Int64 needCount,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_offsets.resize(offsets.size());
        if (delim.empty() || needCount == 0)
        {
            // All result is ""
            res_data.resize(offsets.size());
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                res_data[i] = '\0';
                res_offsets[i] = i + 1;
            }
            return;
        }

        ColumnString::Offset res_offset = 0;
        Volnitsky searcher(delim.c_str(), delim.size(), 0);
        res_data.reserve(data.size());
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i) - 1;

            subStringIndex(&data[data_offset], data_size, &searcher, delim.size(), needCount, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    template <typename IntType>
    static void vectorVectorVector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const ColumnString::Chars_t & delim_data,
        const ColumnString::Offsets & delim_offsets,
        const PaddedPODArray<IntType> & needCount,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i) - 1;
            auto delim_offset = StringUtil::offsetAt(delim_offsets, i);
            auto delim_size = StringUtil::sizeAt(delim_offsets, i) - 1; // ignore the trailing zero.
            Int64 count = accurate::lessOp(INT64_MAX, needCount[i]) ? INT64_MAX : needCount[i];

            if (delim_size == 0 || count == 0)
            {
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = '\0';
                ++res_offset;
                res_offsets[i] = res_offset;
                continue;
            }
            Volnitsky searcher(reinterpret_cast<const char *>(&delim_data[delim_offset]), delim_size, data_size);
            subStringIndex(&data[data_offset], data_size, &searcher, delim_size, count, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    static void subStringIndex(
        const UInt8 * data_begin,
        size_t data_size,
        Volnitsky * delim_searcher,
        size_t delim_size,
        Int64 count,
        ColumnString::Chars_t & res_data,
        ColumnString::Offset & res_offset)
    {
        const UInt8 * begin = data_begin;
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data_size;
        assert(delim_size != 0);
        if (count > 0)
        {
            // Fast exit when count * delim_size > data_size
            if (static_cast<Int64>(data_size / delim_size) < count)
            {
                copyDataToResult(res_data, res_offset, begin, end);
                return;
            }
            while (pos < end)
            {
                const UInt8 * match = delim_searcher->search(pos, end - pos);
                --count;
                if (match == end || count == 0)
                {
                    copyDataToResult(res_data, res_offset, begin, match);
                    break;
                }
                pos = match + delim_size;
            }
        }
        else
        {
            std::vector<const UInt8 *> delim_pos;
            // Fast exit when count * delim_size > data_size, or count == INT64_MIN
            if (count == std::numeric_limits<Int64>::min() || static_cast<Int64>(data_size / delim_size) < -count)
            {
                copyDataToResult(res_data, res_offset, begin, end);
                return;
            }
            count = -count;
            // When count is negative, we need split string by delim.
            while (pos < end)
            {
                const UInt8 * match = delim_searcher->search(pos, end - pos);
                if (match == end)
                {
                    break;
                }
                delim_pos.push_back(match);
                pos = match + delim_size;
            }

            if (count <= static_cast<Int64>(delim_pos.size()))
            {
                auto delim_count = delim_pos.size();
                const UInt8 * match = delim_pos[delim_count - count];
                begin = match + delim_size;
            }
            copyDataToResult(res_data, res_offset, begin, end);
        }
    }

    static void copyDataToResult(
        ColumnString::Chars_t & res_data,
        ColumnString::Offset & res_offset,
        const UInt8 * begin,
        const UInt8 * end)
    {
        res_data.resize(res_data.size() + (end - begin + 1));
        memcpy(&res_data[res_offset], begin, end - begin);
        res_data[res_offset + (end - begin)] = '\0';
        res_offset += end - begin + 1;
    }
};

template <typename Name, typename Format>
class FormatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit FormatImpl(const Context & context_)
        : context(context_)
    {}

    static FunctionPtr create(const Context & context_)
    {
        return std::make_shared<FormatImpl>(context_);
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto first_argument = arguments[0];
        if (!first_argument->isNumber() && !first_argument->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", first_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    /// string format(number/decimal, int/uint)
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & number_base_type = block.getByPosition(arguments[0]).type;
        const auto & precision_base_type = block.getByPosition(arguments[1]).type;

        auto col_res = ColumnString::create();
        auto val_num = block.getByPosition(arguments[0]).column->size();

        bool is_types_valid = getNumberType(number_base_type, [&](const auto & number_type, bool) {
            using NumberType = std::decay_t<decltype(number_type)>;
            using NumberFieldType = typename NumberType::FieldType;
            using NumberColVec = std::conditional_t<IsDecimal<NumberFieldType>, ColumnDecimal<NumberFieldType>, ColumnVector<NumberFieldType>>;
            const auto * number_raw = block.getByPosition(arguments[0]).column.get();
            TiDBDecimalRoundInfo info{number_type, number_type};

            return getPrecisionType(precision_base_type, [&](const auto & precision_type, bool) {
                using PrecisionType = std::decay_t<decltype(precision_type)>;
                using PrecisionFieldType = typename PrecisionType::FieldType;
                using PrecisionColVec = ColumnVector<PrecisionFieldType>;
                const auto * precision_raw = block.getByPosition(arguments[1]).column.get();

                if (const auto * col0_const = checkAndGetColumnConst<NumberColVec>(number_raw))
                {
                    const NumberFieldType & const_number = col0_const->template getValue<NumberFieldType>();

                    if (const auto * col1_column = checkAndGetColumn<PrecisionColVec>(precision_raw))
                    {
                        const auto & precision_array = col1_column->getData();
                        for (size_t i = 0; i != val_num; ++i)
                        {
                            size_t max_num_decimals = getMaxNumDecimals(precision_array[i]);
                            format(const_number, max_num_decimals, info, col_res->getChars(), col_res->getOffsets());
                        }
                    }
                    else
                        return false;
                }
                else if (const auto * col0_column = checkAndGetColumn<NumberColVec>(number_raw))
                {
                    if (const auto * col1_const = checkAndGetColumnConst<PrecisionColVec>(precision_raw))
                    {
                        size_t max_num_decimals = getMaxNumDecimals(col1_const->template getValue<PrecisionFieldType>());
                        for (const auto & number : col0_column->getData())
                            format(number, max_num_decimals, info, col_res->getChars(), col_res->getOffsets());
                    }
                    else if (const auto * col1_column = checkAndGetColumn<PrecisionColVec>(precision_raw))
                    {
                        const auto & number_array = col0_column->getData();
                        const auto & precision_array = col1_column->getData();
                        for (size_t i = 0; i != val_num; ++i)
                        {
                            size_t max_num_decimals = getMaxNumDecimals(precision_array[i]);
                            format(number_array[i], max_num_decimals, info, col_res->getChars(), col_res->getOffsets());
                        }
                    }
                    else
                        return false;
                }
                else
                    return false;

                block.getByPosition(result).column = std::move(col_res);
                return true;
            });
        });

        if (!is_types_valid)
            throw Exception(
                fmt::format("Illegal types {}, {} arguments of function {}", number_base_type->getName(), precision_base_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    const Context & context;

    /// format_max_decimals limits the maximum number of decimal digits for result of
    /// function `format`, this value is same as `FORMAT_MAX_DECIMALS` in MySQL source code.
    static constexpr size_t format_max_decimals = 30;

    template <typename F>
    static bool getNumberType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeDecimal32,
            DataTypeDecimal64,
            DataTypeDecimal128,
            DataTypeDecimal256,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }

    template <typename F>
    static bool getPrecisionType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }

    template <typename T>
    static size_t getMaxNumDecimals(T precision)
    {
        static_assert(std::is_integral_v<T>);
        if (accurate::lessOrEqualsOp(precision, 0))
            return 0;
        return std::min(static_cast<size_t>(precision), format_max_decimals);
    }

    template <typename T>
    static T round(T number, size_t max_num_decimals [[maybe_unused]], const TiDBDecimalRoundInfo & info [[maybe_unused]])
    {
        if constexpr (IsDecimal<T>)
            return TiDBDecimalRound<T, T>::eval(number, max_num_decimals, info);
        else if constexpr (std::is_floating_point_v<T>)
            return TiDBFloatingRound<T, T>::eval(number, max_num_decimals);
        else
        {
            static_assert(std::is_integral_v<T>);
            return number;
        }
    }

    template <typename T>
    static std::string number2Str(T number, const TiDBDecimalRoundInfo & info [[maybe_unused]])
    {
        if constexpr (IsDecimal<T>)
            return number.toString(info.output_scale);
        else
        {
            static_assert(std::is_floating_point_v<T> || std::is_integral_v<T>);
            return fmt::format("{}", number);
        }
    }

    static void copyFromBuffer(const std::string & buffer, ColumnString::Chars_t & res_data, ColumnString::Offsets & res_offsets)
    {
        const size_t old_size = res_data.size();
        const size_t size_to_append = buffer.size() + 1;
        const size_t new_size = old_size + size_to_append;

        res_data.resize(new_size);
        memcpy(&res_data[old_size], buffer.c_str(), size_to_append);
        res_offsets.push_back(new_size);
    }

    template <typename T>
    static void format(
        T number,
        size_t max_num_decimals,
        const TiDBDecimalRoundInfo & info,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        T round_number = round(number, max_num_decimals, info);
        std::string round_number_str = number2Str(round_number, info);
        std::string buffer = Format::apply(round_number_str, max_num_decimals);
        copyFromBuffer(buffer, res_data, res_offsets);
    }
};

struct FormatWithEnUS
{
    static std::string apply(const std::string & number, size_t precision)
    {
        std::string buffer;
        size_t number_part_start = 0;
        if (number[0] == '-')
        {
            buffer += '-';
            number_part_start = 1;
        }

        auto point_index = number.find('.');
        if (point_index == std::string::npos)
            point_index = number.size();

        /// a comma can be used to group 3 digits in en_US locale, such as 12,345,678.00
        constexpr int digit_grouping_size = 3;
        constexpr char comma = ',';
        auto integer_part_size = point_index - number_part_start;
        const auto remainder = integer_part_size % digit_grouping_size;
        auto integer_part_pos = number.cbegin() + number_part_start;
        if (remainder != 0)
        {
            buffer.append(integer_part_pos, integer_part_pos + remainder);
            buffer += comma;
            integer_part_pos += remainder;
        }
        const auto integer_part_end = number.cbegin() + point_index;
        for (; integer_part_pos != integer_part_end; integer_part_pos += digit_grouping_size)
        {
            buffer.append(integer_part_pos, integer_part_pos + digit_grouping_size);
            buffer += comma;
        }
        buffer.pop_back();

        if (precision > 0)
        {
            buffer += '.';
            if (point_index == number.size()) /// no decimal part
                buffer.append(precision, '0');
            else
            {
                const auto decimal_part_size = number.size() - point_index - 1;
                const auto decimal_part_start = integer_part_end + 1;
                if (decimal_part_size >= precision)
                    buffer.append(decimal_part_start, decimal_part_start + precision);
                else
                    buffer.append(decimal_part_start, number.cend()).append(precision - decimal_part_size, '0');
            }
        }
        return buffer;
    }
};

class FunctionFormatWithLocale : public IFunction
{
public:
    struct NameFormatWithLocale
    {
        static constexpr auto name = "formatWithLocale";
    };
    template <typename Format>
    using FormatImpl_t = FormatImpl<NameFormatWithLocale, Format>;

    static constexpr auto name = NameFormatWithLocale::name;
    explicit FunctionFormatWithLocale(const Context & context_)
        : context(checkDagContextIsValid(context_))
    {}

    static FunctionPtr create(const Context & context_)
    {
        return std::make_shared<FunctionFormatWithLocale>(context_);
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto first_argument = removeNullable(arguments[0]);
        if (!first_argument->isNumber() && !first_argument->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", first_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto second_argument = removeNullable(arguments[1]);
        if (!second_argument->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", second_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto third_argument = removeNullable(arguments[2]);
        if (!third_argument->isString())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", third_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto return_type = std::make_shared<DataTypeString>();
        return (arguments[0]->isNullable() || arguments[1]->isNullable()) ? makeNullable(return_type) : return_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * locale_raw = block.getByPosition(arguments[2]).column.get();
        handleLocale(locale_raw);

        /// TODO support switching different locale in a block.
        static DefaultExecutable forward_function{std::make_shared<FormatImpl_t<FormatWithEnUS>>(context)};
        const ColumnNumbers forward_arguments{arguments[0], arguments[1]};
        forward_function.execute(block, forward_arguments, result);
    }

private:
    const Context & context;

    /// Append warning when locale is not 'en_US'.
    /// TODO support other locales after tidb has supported them.
    void handleLocale(const IColumn * locale_raw) const
    {
        static const std::string supported_locale = "en_US";
        using LocaleColVec = ColumnString;
        const auto column_size = locale_raw->size();
        if (const auto * locale_const = checkAndGetColumnConst<LocaleColVec>(locale_raw, true))
        {
            if (locale_const->onlyNull())
            {
                const auto & msg = genWarningMsg("NULL");
                for (size_t i = 0; i != column_size; ++i)
                    context.getDAGContext()->appendWarning(msg);
            }
            else
            {
                const String value = locale_const->getValue<String>();
                if (!boost::iequals(value, supported_locale))
                {
                    const auto & msg = genWarningMsg(value);
                    for (size_t i = 0; i != column_size; ++i)
                        context.getDAGContext()->appendWarning(msg);
                }
            }
        }
        else
        {
            Field locale_field;
            for (size_t i = 0; i != column_size; ++i)
            {
                locale_raw->get(i, locale_field);
                if (locale_field.isNull())
                    context.getDAGContext()->appendWarning(genWarningMsg("NULL"));
                else
                {
                    String value = locale_field.get<String>();
                    if (!boost::iequals(value, supported_locale))
                        context.getDAGContext()->appendWarning(genWarningMsg(value));
                }
            }
        }
    }

    static std::string genWarningMsg(const std::string & value)
    {
        return fmt::format("Unknown locale: \'{}\'", value);
    }

    static const Context & checkDagContextIsValid(const Context & context_)
    {
        if (!context_.getDAGContext())
            throw Exception("DAGContext should not be nullptr.", ErrorCodes::LOGICAL_ERROR);
        return context_;
    }
};

// clang-format off
struct NameEmpty                 { static constexpr auto name = "empty"; };
struct NameNotEmpty              { static constexpr auto name = "notEmpty"; };
struct NameLength                { static constexpr auto name = "length"; };
struct NameLengthUTF8            { static constexpr auto name = "lengthUTF8"; };
struct NameLowerUTF8             { static constexpr auto name = "lowerUTF8"; };
struct NameUpperBinary           { static constexpr auto name = "upperBinary"; };
struct NameUpperUTF8             { static constexpr auto name = "upperUTF8"; };
struct NameReverseUTF8           { static constexpr auto name = "reverseUTF8"; };
struct NameTrim                  { static constexpr auto name = "trim"; };
struct NameLTrim                 { static constexpr auto name = "ltrim"; };
struct NameRTrim                 { static constexpr auto name = "rtrim"; };
struct NameTrimUTF8              { static constexpr auto name = "trimUTF8"; };
struct NameLTrimUTF8             { static constexpr auto name = "ltrimUTF8"; };
struct NameRTrimUTF8             { static constexpr auto name = "rtrimUTF8"; };
struct NameLPad                  { static constexpr auto name = "lpad"; };
struct NameLPadUTF8              { static constexpr auto name = "lpadUTF8"; };
struct NameRPad                  { static constexpr auto name = "rpad"; };
struct NameRPadUTF8              { static constexpr auto name = "rpadUTF8"; };
struct NameConcat                { static constexpr auto name = "concat"; };
struct NameConcatAssumeInjective { static constexpr auto name = "concatAssumeInjective"; };
struct NameFormat                { static constexpr auto name = "format"; };
// clang-format on

using FunctionEmpty = FunctionStringOrArrayToT<EmptyImpl<false>, NameEmpty, UInt8>;
using FunctionNotEmpty = FunctionStringOrArrayToT<EmptyImpl<true>, NameNotEmpty, UInt8>;
// using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64>;
using FunctionLengthUTF8 = FunctionStringOrArrayToT<LengthUTF8Impl, NameLengthUTF8, UInt64>;
using FunctionLowerBinary = FunctionStringToString<TiDBLowerUpperBinaryImpl, NameLowerBinary>;
using FunctionLowerUTF8 = FunctionStringToString<TiDBLowerUpperUTF8Impl<'A', 'Z', CharUtil::unicodeToLower>, NameLowerUTF8>;
using FunctionUpperBinary = FunctionStringToString<TiDBLowerUpperBinaryImpl, NameUpperBinary>;
using FunctionUpperUTF8 = FunctionStringToString<TiDBLowerUpperUTF8Impl<'a', 'z', CharUtil::unicodeToUpper>, NameUpperUTF8>;
using FunctionReverseUTF8 = FunctionStringToString<ReverseUTF8Impl, NameReverseUTF8, true>;
using FunctionTrimUTF8 = TrimUTF8Impl<NameTrim, true, true>;
using FunctionLTrimUTF8 = TrimUTF8Impl<NameLTrim, true, false>;
using FunctionRTrimUTF8 = TrimUTF8Impl<NameRTrim, false, true>;
using FunctionLPadUTF8 = PadUTF8Impl<NameLPad, true>;
using FunctionRPadUTF8 = PadUTF8Impl<NameRPad, false>;
using FunctionConcat = ConcatImpl<NameConcat, false>;
using FunctionConcatAssumeInjective = ConcatImpl<NameConcatAssumeInjective, true>;
using FunctionFormat = FormatImpl<NameFormat, FormatWithEnUS>;


void registerFunctionsString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmpty>();
    factory.registerFunction<FunctionNotEmpty>();
    factory.registerFunction<FunctionLength>();
    factory.registerFunction<FunctionLengthUTF8>();
    factory.registerFunction<FunctionLowerBinary>();
    factory.registerFunction<FunctionUpperBinary>();
    factory.registerFunction<FunctionLowerUTF8>();
    factory.registerFunction<FunctionUpperUTF8>();
    factory.registerFunction<FunctionReverse>();
    factory.registerFunction<FunctionReverseUTF8>();
    factory.registerFunction<FunctionTrimUTF8>();
    factory.registerFunction<FunctionLTrimUTF8>();
    factory.registerFunction<FunctionRTrimUTF8>();
    factory.registerFunction<FunctionLPadUTF8>();
    factory.registerFunction<FunctionRPadUTF8>();
    factory.registerFunction<FunctionConcat>();
    factory.registerFunction<FunctionConcatAssumeInjective>();
    factory.registerFunction<FunctionTiDBConcat>();
    factory.registerFunction<FunctionTiDBConcatWithSeparator>();
    factory.registerFunction<FunctionSubstring>();
    factory.registerFunction<FunctionSubstringUTF8>();
    factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
    factory.registerFunction<FunctionJsonLength>();
    factory.registerFunction<FunctionRightUTF8>();
    factory.registerFunction<FunctionASCII>();
    factory.registerFunction<FunctionPosition>();
    factory.registerFunction<FunctionSubStringIndex>();
    factory.registerFunction<FunctionFormat>();
    factory.registerFunction<FunctionFormatWithLocale>();
}
} // namespace DB

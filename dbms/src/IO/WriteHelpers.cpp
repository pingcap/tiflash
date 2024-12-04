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

#include <Common/hex.h>
#include <IO/WriteHelpers.h>
#include <inttypes.h>

#include <charconv>


namespace DB
{
template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, size_t num_bytes)
{
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; src_pos < num_bytes; ++src_pos)
    {
        writeHexByteLowercase(src[src_pos], &dst[dst_pos]);
        dst_pos += 2;
    }
}

void formatUUID(const UInt8 * src16, UInt8 * dst36)
{
    formatHex(&src16[0], &dst36[0], 4);
    dst36[8] = '-';
    formatHex(&src16[4], &dst36[9], 2);
    dst36[13] = '-';
    formatHex(&src16[6], &dst36[14], 2);
    dst36[18] = '-';
    formatHex(&src16[8], &dst36[19], 2);
    dst36[23] = '-';
    formatHex(&src16[10], &dst36[24], 6);
}

/** Function used when byte ordering is important when parsing uuid
 *  ex: When we create an UUID type
 */
void formatUUID(std::reverse_iterator<const UInt8 *> src16, UInt8 * dst36)
{
    formatHex(src16 + 8, &dst36[0], 4);
    dst36[8] = '-';
    formatHex(src16 + 12, &dst36[9], 2);
    dst36[13] = '-';
    formatHex(src16 + 14, &dst36[14], 2);
    dst36[18] = '-';
    formatHex(src16, &dst36[19], 2);
    dst36[23] = '-';
    formatHex(src16 + 2, &dst36[24], 6);
}


void writeException(const Exception & e, WriteBuffer & buf)
{
    writeBinary(e.code(), buf);
    writeBinary(String(e.name()), buf);
    writeBinary(e.displayText(), buf);
    writeBinary(e.getStackTrace().toString(), buf);

    bool has_nested = e.nested() != nullptr;
    writeBinary(has_nested, buf);

    if (has_nested)
        writeException(Exception(*e.nested()), buf);
}

void writePointerHex(const void * ptr, WriteBuffer & buf)
{
    writeString("0x", buf);
    char hex_str[2 * sizeof(ptr)];
    writeHexUIntLowercase(reinterpret_cast<uintptr_t>(ptr), hex_str);
    buf.write(hex_str, 2 * sizeof(ptr));
}

template <typename T>
void writeFloatTextNoExp(T x, WriteBuffer & buf)
{
    static_assert(
        std::is_same_v<T, double> || std::is_same_v<T, float>,
        "Argument for writeFloatText must be float or double");

    using Converter = DoubleConverter<false>;

    Converter::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    bool result = false;
    if constexpr (std::is_same_v<T, double>)
        result = Converter::instance().ToShortest(x, &builder);
    else
        result = Converter::instance().ToShortestSingle(x, &builder);

    if (!result)
        throw Exception("Cannot print floating point number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    constexpr std::string_view nan = "NaN";
    constexpr std::string_view neg_inf = "-Inf";
    constexpr std::string_view inf = "+Inf";

    std::string_view sv{buffer, static_cast<size_t>(builder.position())};
    if (sv == "nan")
    {
        buf.write(nan.data(), nan.size());
        return;
    }
    else if (sv == "-inf")
    {
        buf.write(neg_inf.data(), neg_inf.size());
        return;
    }
    else if (sv == "inf")
    {
        buf.write(inf.data(), inf.size());
        return;
    }

    constexpr auto c_neg = '-';
    constexpr auto c_zero = '0';
    constexpr auto c_dot = '.';
    constexpr auto c_exp = 'e';

    bool neg = buffer[0] == c_neg;
    int bg = 0, ed = builder.position();
    if (neg)
    {
        bg++;
    }

    // return zero
    if (ed - bg == 1 && sv[bg] == c_zero)
    {
        buf.write(sv.data(), sv.size());
        return;
    }

    Int64 exp_pos = sv.find(c_exp);
    if (exp_pos < 0)
    {
        buf.write(sv.data(), sv.size());
        return;
    }

    Int64 exp10 = 0;
    if (exp_pos >= 0)
    {
        auto exp_sv = sv.substr(exp_pos + 1);
        std::from_chars(exp_sv.begin(), exp_sv.end(), exp10);
        ed = exp_pos;
    }
    auto int_bg = bg, int_ed = ed, float_bg = ed, float_ed = ed;

    if (const auto begin = sv.data() + bg, end = sv.data() + ed, dot_pos = std::find(begin, end, c_dot); dot_pos != end)
    {
        int_ed = dot_pos - sv.data();
        float_bg = int_ed + 1;
    }


    if (int_ed - int_bg > 1)
    {
        exp10 += int_ed - (int_bg + 1);
    }
    else if (sv[int_bg] == c_zero)
    {
        int_bg += 1;

        auto new_float_bg = float_bg;
        for (auto i = float_bg; i != float_ed; ++i)
        {
            exp10 -= 1;
            if (sv[i] != c_zero)
            {
                new_float_bg = i;
                break;
            }
        }

        float_bg = new_float_bg;
    }

    const auto put_char = [&buf](char c) {
        buf.write(c);
    };
    const auto put_zero = [&]() {
        put_char(c_zero);
    };
    const auto put_dot = [&]() {
        put_char(c_dot);
    };
    const auto put_slice = [&buf](std::string_view s) {
        buf.write(s.data(), s.size());
    };

    if (neg)
    {
        put_char(c_neg);
    }

    if (exp10 < 0)
    {
        exp10 = -exp10;
        put_zero();
        put_dot();
        exp10 -= 1;
        while (exp10 != 0)
        {
            put_zero();
            exp10 -= 1;
        }
        put_slice({sv.data() + int_bg, sv.data() + int_ed});
        put_slice({sv.data() + float_bg, sv.data() + float_ed});
    }
    else
    {
        put_slice({sv.data() + int_bg, sv.data() + int_ed});

        if (exp10 < (float_ed - float_bg))
        {
            put_slice({sv.data() + float_bg, sv.data() + float_bg + exp10});

            put_dot();
            float_bg += exp10;
            put_slice({sv.data() + float_bg, sv.data() + float_ed});
        }
        else
        {
            put_slice({sv.data() + float_bg, sv.data() + float_ed});
            exp10 -= (float_ed - float_bg);
            while (exp10 != 0)
            {
                put_zero();
                exp10 -= 1;
            }
        }
    }
}

template void writeFloatTextNoExp<Float64>(Float64 x, WriteBuffer & buf);
template void writeFloatTextNoExp<Float32>(Float32 x, WriteBuffer & buf);

} // namespace DB

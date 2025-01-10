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

#include <Poco/String.h>
#include <TiDB/Collation/Collator.h>
#include <TiDB/Collation/CollatorUtils.h>

#include <cassert>
#include <unordered_map>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiDB
{
TiDBCollators dummy_collators;
std::vector<std::string> dummy_sort_key_contaners;
std::string dummy_sort_key_contaner;

ALWAYS_INLINE std::string_view rtrim(const char * s, size_t length)
{
    auto v = std::string_view(s, length);
    return DB::RightTrim(v);
}

using Rune = int32_t;
using StringType = std::vector<Rune>;
constexpr uint8_t b2_mask = 0x1F;
constexpr uint8_t b3_mask = 0x0F;
constexpr uint8_t b4_mask = 0x07;
constexpr uint8_t mb_mask = 0x3F;
inline Rune decodeUtf8Char(const char * s, size_t & offset)
{
    uint8_t b0 = s[offset];
    if (b0 < 0x80)
    {
        auto c = static_cast<Rune>(b0);
        offset += 1;
        return c;
    }
    if (b0 < 0xE0)
    {
        auto c = static_cast<Rune>(b0 & b2_mask) << 6 | static_cast<Rune>(s[1 + offset] & mb_mask);
        offset += 2;
        return c;
    }
    if (b0 < 0xF0)
    {
        auto c = static_cast<Rune>(b0 & b3_mask) << 12 | static_cast<Rune>(s[1 + offset] & mb_mask) << 6
            | static_cast<Rune>(s[2 + offset] & mb_mask);
        offset += 3;
        return c;
    }
    auto c = static_cast<Rune>(b0 & b4_mask) << 18 | static_cast<Rune>(s[1 + offset] & mb_mask) << 12
        | static_cast<Rune>(s[2 + offset] & mb_mask) << 6 | static_cast<Rune>(s[3 + offset] & mb_mask);
    offset += 4;
    return c;
}

template <typename Collator>
class Pattern : public ITiDBCollator::IPattern
{
public:
    void compile(const std::string & pattern, char escape) override
    {
        chars.clear();
        match_types.clear();

        chars.reserve(pattern.length() * sizeof(typename Collator::CharType));
        match_types.reserve(pattern.length() * sizeof(typename Pattern::MatchType));

        size_t offset = 0;
        while (offset < pattern.length())
        {
            MatchType tp;
            auto c = Collator::decodeChar(pattern.data(), offset);
            if (c == escape)
            {
                tp = MatchType::Match;
                if (offset < pattern.length())
                {
                    // use next char to match
                    c = Collator::decodeChar(pattern.data(), offset);
                }
                else
                {
                    // use `escape` to match
                }
            }
            else if (c == '_')
            {
                tp = MatchType::One;
            }
            else if (c == '%')
            {
                tp = MatchType::Any;
            }
            else
            {
                tp = MatchType::Match;
            }
            chars.push_back(c);
            match_types.push_back(tp);
        }
    }

    bool match(const char * s, size_t length) const override
    {
        size_t s_offset = 0, next_s_offset = 0, tmp_s_offset = 0;
        size_t p_idx = 0, next_p_idx = 0;
        while (p_idx < chars.size() || s_offset < length)
        {
            if (p_idx < chars.size())
            {
                switch (match_types[p_idx])
                {
                case Match:
                    if (s_offset < length
                        && Collator::regexEq(Collator::decodeChar(s, tmp_s_offset = s_offset), chars[p_idx]))
                    {
                        p_idx++;
                        s_offset = tmp_s_offset;
                        continue;
                    }
                    break;
                case One:
                    if (s_offset < length)
                    {
                        p_idx++;
                        Collator::decodeChar(s, s_offset);
                        continue;
                    }
                    break;
                case Any:
                    next_p_idx = p_idx;
                    Collator::decodeChar(s, next_s_offset = s_offset);
                    p_idx++;
                    continue;
                }
            }
            if (0 < next_s_offset && next_s_offset <= length)
            {
                p_idx = next_p_idx;
                s_offset = next_s_offset;
                continue;
            }
            return false;
        }
        return true;
    }

private:
    std::vector<typename Collator::CharType> chars;

    enum MatchType
    {
        Match,
        One,
        Any,
    };
    std::vector<MatchType> match_types;
};

template <typename T, bool padding = false>
class BinCollator final : public ITiDBCollator
{
public:
    explicit BinCollator(int32_t id)
        : ITiDBCollator(id)
    {}

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        return DB::BinCollatorCompare<padding>(s1, length1, s2, length2);
    }

    StringRef sortKey(const char * s, size_t length, std::string &) const override
    {
        return DB::BinCollatorSortKey<padding>(s, length);
    }

    StringRef sortKeyNoTrim(const char * s, size_t length, std::string &) const override
    {
        return convertForBinCollator<false>(s, length, nullptr);
    }

    StringRef convert(const char * s, size_t length, std::string &, std::vector<size_t> * lens) const override
    {
        return convertForBinCollator<true>(s, length, lens);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<BinCollator<T, padding>>>(); }

    size_t maxBytesForOneChar() const override
    {
        // BinCollator only trims trailing spaces,
        // so it does not increase the space required after decoding.
        // Hence, it returns 1 here.
        return 1;
    }

private:
    const std::string name = padding ? "BinaryPadding" : "Binary";

private:
    using WeightType = T;
    using CharType = T;

    static inline CharType decodeChar(const char * s, size_t & offset)
    {
        if constexpr (std::is_same_v<T, char>)
        {
            return s[offset++];
        }
        else
        {
            return decodeUtf8Char(s, offset);
        }
    }

    static inline WeightType weight(CharType c) { return c; }

    static inline bool regexEq(CharType a, CharType b) { return weight(a) == weight(b); }

    friend class Pattern<BinCollator>;
};

namespace GeneralCI
{
using WeightType = uint16_t;
extern const std::array<WeightType, 256 * 256> weight_lut;
} // namespace GeneralCI

class GeneralCICollator final : public ITiDBCollator
{
public:
    explicit GeneralCICollator(int32_t id)
        : ITiDBCollator(id)
    {}

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        auto v1 = rtrim(s1, length1);
        auto v2 = rtrim(s2, length2);

        size_t offset1 = 0, offset2 = 0;
        while (offset1 < v1.length() && offset2 < v2.length())
        {
            auto c1 = decodeChar(s1, offset1);
            auto c2 = decodeChar(s2, offset2);
            auto sk1 = weight(c1);
            auto sk2 = weight(c2);
            auto cmp = sk1 - sk2;
            if (cmp != 0)
                return DB::signum(cmp);
        }

        return (offset1 < v1.length()) - (offset2 < v2.length());
    }

    StringRef convert(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const override
    {
        return convertImpl<true, false>(s, length, container, lens);
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, true>(s, length, container, nullptr);
    }

    StringRef sortKeyNoTrim(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, false>(s, length, container, nullptr);
    }

    template <bool need_len, bool need_trim>
    StringRef convertImpl(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const
    {
        std::string_view v;

        if constexpr (need_trim)
            v = rtrim(s, length);
        else
            v = std::string_view(s, length);

        const auto max_bytes_one_char = maxBytesForOneChar();
        if (length * max_bytes_one_char > container.size())
            container.resize(length * max_bytes_one_char);
        size_t offset = 0;
        size_t total_size = 0;
        size_t v_length = v.length();

        if constexpr (need_len)
        {
            if (lens->capacity() < v_length)
                lens->reserve(v_length);
            lens->resize(0);
        }

        while (offset < v_length)
        {
            auto c = decodeChar(s, offset);
            auto sk = weight(c);
            container[total_size++] = static_cast<char>(sk >> 8);
            container[total_size++] = static_cast<char>(sk);

            if constexpr (need_len)
                lens->push_back(2);
        }

        return StringRef(container.data(), total_size);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<GeneralCICollator>>(); }

    size_t maxBytesForOneChar() const override { return sizeof(WeightType); }

private:
    const std::string name = "GeneralCI";

private:
    using WeightType = GeneralCI::WeightType;
    using CharType = Rune;

    static inline CharType decodeChar(const char * s, size_t & offset) { return decodeUtf8Char(s, offset); }

    static inline WeightType weight(CharType c)
    {
        if (c > 0xFFFF)
            return 0xFFFD;
        return GeneralCI::weight_lut[c & 0xFFFF];
        //return !!(c >> 16) * 0xFFFD + (1 - !!(c >> 16)) * GeneralCI::weight_lut_0400[c & 0xFFFF];
    }

    static inline bool regexEq(CharType a, CharType b) { return weight(a) == weight(b); }

    friend class Pattern<GeneralCICollator>;
};

namespace UnicodeCI
{
using long_weight = struct
{
    uint64_t first;
    uint64_t second;
};
extern const std::array<uint64_t, 256 * 256 + 1> weight_lut_0400;
extern const std::array<uint64_t, 0x2CEA1> weight_lut_0900;
const uint64_t long_weight_rune = 0xFFFD;

const std::array<long_weight, 23> weight_lut_long_0400
    = {long_weight{0x1D6E1DC61D6D0288, 0x000002891E031DC2},
       long_weight{0x1D741DC61D6D0288, 0x0000000002891DCB},
       long_weight{0x1D621E0F1DBE1D70, 0x0000000000001DC6},
       long_weight{0x0E0B1E591E5E1E55, 0x0000000000001E65},
       long_weight{0x1E781E591E7C1E58, 0x0000000000001E72},
       long_weight{0x0E0B1E731E7C1E58, 0x000000001E7A1E65},
       long_weight{0x1E631E7D1E7C1E58, 0x0000000000001E65},
       long_weight{0x1E651E721E781E59, 0x0000000000001E81},
       long_weight{0x1E531E5F1E7A1E59, 0x0000000000001E7C},
       long_weight{0x0E0B1E621E811E5C, 0x0000000000001E72},
       long_weight{0x1E811E5F0E0B1E6B, 0x0000000000001E65},
       long_weight{0x1E651E5E1E521E6C, 0x0000000000001E7A},
       long_weight{0x1E631E781E521E6D, 0x0000000000001E65},
       long_weight{0x1E551E5D1E631E6D, 0x0000000000001E7A},
       long_weight{0x0E0B1E611E591E6E, 0x0000000000001E7A},
       long_weight{0x1E771E5D1E811E70, 0x0000000000001E81},
       long_weight{0x0E0B1E6B1E791E71, 0x0000000000001E7A},
       long_weight{0x1E5A1E651E811E7B, 0x0000000000001E81},
       long_weight{0xDF0FFB40E82AFB40, 0xF93EFB40CF1AFB40},
       long_weight{0x04370E6D0E330FC0, 0x0000000000000FEA},
       long_weight{0x04370E6D0E330FC0, 0x000000000E2B0FEA},
       long_weight{0x135E020913AB135E, 0x13B713AB135013AB},
       // for default use
       long_weight{0x0, 0x0}};

const std::array<long_weight, 28> weight_lut_long_0900
    = {long_weight{0x3C013C7B3C000317, 0x000003183CD43C77},
       long_weight{0x3C073C7B3C000317, 0x0000000003183C80},
       long_weight{0x3BF53CE03C733C03, 0x0000000000003C7B},
       long_weight{0x1C0E3D623D673D5E, 0x0000000000003D6E},
       long_weight{0x3D823D623D863D61, 0x0000000000003D7B},
       long_weight{0x1C0E3D7C3D863D61, 0x000000003D843D6E},
       long_weight{0x3D6C3D873D863D61, 0x0000000000003D6E},
       long_weight{0x3D6E3D7B3D823D62, 0x0000000000003D8B},
       long_weight{0x3D5B3D683D843D62, 0x0000000000003D86},
       long_weight{0x1C0E3D6B3D8B3D65, 0x0000000000003D7B},
       long_weight{0x3D8B3D681C0E3D74, 0x0000000000003D6E},
       long_weight{0x3D6E3D673D5A3D75, 0x0000000000003D84},
       long_weight{0x3D6C3D823D5A3D76, 0x0000000000003D6E},
       long_weight{0x3D5E3D663D6C3D76, 0x0000000000003D84},
       long_weight{0x1C0E3D6A3D623D77, 0x0000000000003D84},
       long_weight{0x3D813D663D8B3D79, 0x0000000000003D8B},
       long_weight{0x1C0E3D743D833D7A, 0x0000000000003D84},
       long_weight{0x3D633D6E3D8B3D85, 0x0000000000003D8B},
       long_weight{0xDF0FFB40E82AFB40, 0xF93EFB40CF1AFB40},
       long_weight{0x06251C8F1C471E33, 0x0000000000001E71},
       long_weight{0x06251C8F1C471E33, 0x000000001C3F1E71},
       long_weight{0x020923C5239C2364, 0x23B1239C239C230B},
       long_weight{0x23250209239C2325, 0x23B1239C230B239C},
       long_weight{0x000000000000FFFD, 0x0000000000000000},
       long_weight{0x02091C8F1DB91C3F, 0x00001E331C7A1E71},
       long_weight{0x1E3302091D321D18, 0x000000001E711CAA},
       long_weight{0x1E711E711DDD1D77, 0x1E711E711CAA1D77},
       // for default use
       long_weight{0x0, 0x0}};
} // namespace UnicodeCI

template <typename T, bool padding>
class UCACICollator final : public ITiDBCollator
{
public:
    explicit UCACICollator(int32_t id)
        : ITiDBCollator(id)
    {}

    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        std::string_view v1 = preprocess(s1, length1), v2 = preprocess(s2, length2);

        size_t offset1 = 0, offset2 = 0;
        size_t v1_length = v1.length(), v2_length = v2.length();

        // since the longest weight of character in unicode ci has 128bit, we divide it to 2 uint64.
        // The xx_first stand for the first 64bit, and the xx_second stand for the second 64bit.
        // If xx_first == 0, there is always has xx_second == 0
        uint64_t s1_first = 0, s1_second = 0;
        uint64_t s2_first = 0, s2_second = 0;

        while (true)
        {
            weight(s1_first, s1_second, offset1, v1_length, s1);
            weight(s2_first, s2_second, offset2, v2_length, s2);

            if (s1_first == 0 || s2_first == 0)
            {
                if (s1_first < s2_first)
                {
                    return -1;
                }
                if (s1_first > s2_first)
                {
                    return 1;
                }
                return 0;
            }

            if (s1_first == s2_first)
            {
                s1_first = 0;
                s2_first = 0;
                continue;
            }

            while (s1_first != 0 && s2_first != 0)
            {
                if (((s1_first ^ s2_first) & 0xFFFF) == 0)
                {
                    s1_first >>= 16;
                    s2_first >>= 16;
                }
                else
                {
                    return DB::signum(static_cast<int>(s1_first & 0xFFFF) - static_cast<int>(s2_first & 0xFFFF));
                }
            }
        }
    }

    StringRef convert(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const override
    {
        return convertImpl<true, false>(s, length, container, lens);
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, true>(s, length, container, nullptr);
    }

    StringRef sortKeyNoTrim(const char * s, size_t length, std::string & container) const override
    {
        return convertImpl<false, false>(s, length, container, nullptr);
    }

    template <bool need_len, bool need_trim>
    StringRef convertImpl(const char * s, size_t length, std::string & container, std::vector<size_t> * lens) const
    {
        std::string_view v;

        if constexpr (need_trim)
            v = preprocess(s, length);
        else
            v = std::string_view(s, length);

        const auto max_bytes_one_char = maxBytesForOneChar();
        if (length * max_bytes_one_char > container.size())
            container.resize(length * max_bytes_one_char);
        size_t offset = 0;
        size_t total_size = 0;
        size_t v_length = v.length();

        uint64_t first = 0, second = 0;

        if constexpr (need_len)
        {
            if (lens->capacity() < v_length)
                lens->reserve(v_length);
            lens->resize(0);
        }

        while (offset < v_length)
        {
            weight(first, second, offset, v_length, s);

            if constexpr (need_len)
                lens->push_back(total_size);

            writeResult(first, container, total_size);
            writeResult(second, container, total_size);

            if constexpr (need_len)
            {
                size_t end_idx = lens->size() - 1;
                (*lens)[end_idx] = total_size - (*lens)[end_idx];
            }
        }

        return StringRef(container.data(), total_size);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<UCACICollator>>(); }

    size_t maxBytesForOneChar() const override
    {
        // Every char have 8 uint16 at most.
        return 8 * sizeof(uint16_t);
    }

private:
    const std::string name = "UnicodeCI";

private:
    using CharType = Rune;

    static inline CharType decodeChar(const char * s, size_t & offset) { return decodeUtf8Char(s, offset); }

    static inline void writeResult(uint64_t & w, std::string & container, size_t & total_size)
    {
        while (w != 0)
        {
            container[total_size++] = static_cast<char>(w >> 8);
            container[total_size++] = static_cast<char>(w);
            w >>= 16;
        }
    }

    static inline bool regexEq(CharType a, CharType b) { return T::regexEq(a, b); }

    static inline void weight(uint64_t & first, uint64_t & second, size_t & offset, size_t length, const char * s)
    {
        if (first == 0)
        {
            if (second == 0)
            {
                while (offset < length)
                {
                    auto r = decodeChar(s, offset);
                    if (!T::weight(first, second, r))
                    {
                        continue;
                    }
                    break;
                }
            }
            else
            {
                first = second;
                second = 0;
            }
        }
    }

    static inline std::string_view preprocess(const char * s, size_t length)
    {
        if constexpr (padding)
        {
            return rtrim(s, length);
        }

        return std::string_view(s, length);
    }

    friend class Pattern<UCACICollator>;
};

class Unicode0400
{
public:
    static inline bool regexEq(Rune a, Rune b)
    {
        if (a > 0xFFFF || b > 0xFFFF)
        {
            return a == b;
        }

        auto a_weight = UnicodeCI::weight_lut_0400[a];
        auto b_weight = UnicodeCI::weight_lut_0400[b];

        if (a_weight != b_weight)
        {
            return false;
        }

        if (a_weight == UnicodeCI::long_weight_rune)
        {
            return a == b;
        }

        return true;
    }

    static inline bool weight(uint64_t & first, uint64_t & second, Rune r)
    {
        if (r > 0xFFFF)
        {
            first = 0xFFFD;
            return true;
        }
        auto w = UnicodeCI::weight_lut_0400[r];
        // skip 0 weight char
        if (w == 0)
        {
            return false;
        }
        if (w == UnicodeCI::long_weight_rune)
        {
            auto long_weight = weightLutLongMap(r);
            first = long_weight.first;
            second = long_weight.second;
        }
        else
        {
            first = w;
        }
        return true;
    }

private:
    static inline const UnicodeCI::long_weight & weightLutLongMap(Rune r)
    {
        switch (r)
        {
        case 0x321D:
            return UnicodeCI::weight_lut_long_0400[0];
        case 0x321E:
            return UnicodeCI::weight_lut_long_0400[1];
        case 0x327C:
            return UnicodeCI::weight_lut_long_0400[2];
        case 0x3307:
            return UnicodeCI::weight_lut_long_0400[3];
        case 0x3315:
            return UnicodeCI::weight_lut_long_0400[4];
        case 0x3316:
            return UnicodeCI::weight_lut_long_0400[5];
        case 0x3317:
            return UnicodeCI::weight_lut_long_0400[6];
        case 0x3319:
            return UnicodeCI::weight_lut_long_0400[7];
        case 0x331A:
            return UnicodeCI::weight_lut_long_0400[8];
        case 0x3320:
            return UnicodeCI::weight_lut_long_0400[9];
        case 0x332B:
            return UnicodeCI::weight_lut_long_0400[10];
        case 0x332E:
            return UnicodeCI::weight_lut_long_0400[11];
        case 0x3332:
            return UnicodeCI::weight_lut_long_0400[12];
        case 0x3334:
            return UnicodeCI::weight_lut_long_0400[13];
        case 0x3336:
            return UnicodeCI::weight_lut_long_0400[14];
        case 0x3347:
            return UnicodeCI::weight_lut_long_0400[15];
        case 0x334A:
            return UnicodeCI::weight_lut_long_0400[16];
        case 0x3356:
            return UnicodeCI::weight_lut_long_0400[17];
        case 0x337F:
            return UnicodeCI::weight_lut_long_0400[18];
        case 0x33AE:
            return UnicodeCI::weight_lut_long_0400[19];
        case 0x33AF:
            return UnicodeCI::weight_lut_long_0400[20];
        case 0xFDFB:
            return UnicodeCI::weight_lut_long_0400[21];
        default:
            return UnicodeCI::weight_lut_long_0400[22];
        }
    }
};

class Unicode0900
{
public:
    static inline bool regexEq(Rune a, Rune b)
    {
        uint64_t a_first = 0, a_second = 0, b_first = 0, b_second = 0;
        weight(a_first, a_second, a);
        weight(b_first, b_second, b);

        return a_first == b_first && a_second == b_second;
    }

    static inline bool weight(uint64_t & first, uint64_t & second, Rune r)
    {
        if (r > 0x2CEA1)
        {
            first = (r >> 15) + 0xFBC0 + (((r & 0x7FFF) | 0x8000) << 16);
            return true;
        }

        auto w = UnicodeCI::weight_lut_0900[r];
        // skip 0 weight char
        if (w == 0)
        {
            return false;
        }
        if (w == UnicodeCI::long_weight_rune)
        {
            auto long_weight = weightLutLongMap(r);
            first = long_weight.first;
            second = long_weight.second;
        }
        else
        {
            first = w;
        }
        return true;
    }

private:
    static inline const UnicodeCI::long_weight & weightLutLongMap(Rune r)
    {
        switch (r)
        {
        case 0x321D:
            return UnicodeCI::weight_lut_long_0900[0];
        case 0x321E:
            return UnicodeCI::weight_lut_long_0900[1];
        case 0x327C:
            return UnicodeCI::weight_lut_long_0900[2];
        case 0x3307:
            return UnicodeCI::weight_lut_long_0900[3];
        case 0x3315:
            return UnicodeCI::weight_lut_long_0900[4];
        case 0x3316:
            return UnicodeCI::weight_lut_long_0900[5];
        case 0x3317:
            return UnicodeCI::weight_lut_long_0900[6];
        case 0x3319:
            return UnicodeCI::weight_lut_long_0900[7];
        case 0x331A:
            return UnicodeCI::weight_lut_long_0900[8];
        case 0x3320:
            return UnicodeCI::weight_lut_long_0900[9];
        case 0x332B:
            return UnicodeCI::weight_lut_long_0900[10];
        case 0x332E:
            return UnicodeCI::weight_lut_long_0900[11];
        case 0x3332:
            return UnicodeCI::weight_lut_long_0900[12];
        case 0x3334:
            return UnicodeCI::weight_lut_long_0900[13];
        case 0x3336:
            return UnicodeCI::weight_lut_long_0900[14];
        case 0x3347:
            return UnicodeCI::weight_lut_long_0900[15];
        case 0x334A:
            return UnicodeCI::weight_lut_long_0900[16];
        case 0x3356:
            return UnicodeCI::weight_lut_long_0900[17];
        case 0x337F:
            return UnicodeCI::weight_lut_long_0900[18];
        case 0x33AE:
            return UnicodeCI::weight_lut_long_0900[19];
        case 0x33AF:
            return UnicodeCI::weight_lut_long_0900[20];
        case 0xFDFA:
            return UnicodeCI::weight_lut_long_0900[21];
        case 0xFDFB:
            return UnicodeCI::weight_lut_long_0900[22];
        case 0xFFFD:
            return UnicodeCI::weight_lut_long_0900[23];
        case 0x1F19C:
            return UnicodeCI::weight_lut_long_0900[24];
        case 0x1F1A8:
            return UnicodeCI::weight_lut_long_0900[25];
        case 0x1F1A9:
            return UnicodeCI::weight_lut_long_0900[26];
        default:
            return UnicodeCI::weight_lut_long_0400[27];
        }
    }
};

struct TiDBCollatorTypeIDMap
{
    TiDBCollatorTypeIDMap()
    {
        id_to_type[ITiDBCollator::UTF8_GENERAL_CI] = ITiDBCollator::CollatorType::UTF8_GENERAL_CI;
        id_to_type[ITiDBCollator::UTF8MB4_GENERAL_CI] = ITiDBCollator::CollatorType::UTF8MB4_GENERAL_CI;
        id_to_type[ITiDBCollator::UTF8_UNICODE_CI] = ITiDBCollator::CollatorType::UTF8_UNICODE_CI;
        id_to_type[ITiDBCollator::UTF8MB4_UNICODE_CI] = ITiDBCollator::CollatorType::UTF8MB4_UNICODE_CI;
        id_to_type[ITiDBCollator::UTF8MB4_0900_AI_CI] = ITiDBCollator::CollatorType::UTF8MB4_0900_AI_CI;
        id_to_type[ITiDBCollator::UTF8MB4_0900_BIN] = ITiDBCollator::CollatorType::UTF8MB4_0900_BIN;
        id_to_type[ITiDBCollator::UTF8MB4_BIN] = ITiDBCollator::CollatorType::UTF8MB4_BIN;
        id_to_type[ITiDBCollator::LATIN1_BIN] = ITiDBCollator::CollatorType::LATIN1_BIN;
        id_to_type[ITiDBCollator::BINARY] = ITiDBCollator::CollatorType::BINARY;
        id_to_type[ITiDBCollator::ASCII_BIN] = ITiDBCollator::CollatorType::ASCII_BIN;
        id_to_type[ITiDBCollator::UTF8_BIN] = ITiDBCollator::CollatorType::UTF8_BIN;
    }

    const ITiDBCollator::CollatorType & operator[](int32_t n) const { return id_to_type.at(n); }

private:
    std::unordered_map<int32_t, ITiDBCollator::CollatorType> id_to_type;
};

static const TiDBCollatorTypeIDMap tidb_collator_type_id_map;

ITiDBCollator::ITiDBCollator(int32_t collator_id_)
    : collator_id(collator_id_)
    , collator_type(tidb_collator_type_id_map[collator_id_])
{}

using UTF8MB4_BIN_TYPE = BinCollator<Rune, true>;
using UTF8MB4_0900_BIN_TYPE = BinCollator<Rune, false>;

struct TiDBCollatorPtrMap
{
    // static constexpr auto MAX_TYPE_CNT = static_cast<uint32_t>(ITiDBCollator::CollatorType::MAX_);

    std::unordered_map<int32_t, TiDBCollatorPtr> id_map;
    // std::array<TiDBCollatorPtr, MAX_TYPE_CNT> type_map{};
    std::unordered_map<std::string, TiDBCollatorPtr> name_map;
    std::unordered_map<const void *, ITiDBCollator::CollatorType> addr_to_type;

    TiDBCollatorPtrMap()
    {
        static const auto c_utf8_general_ci = GeneralCICollator(ITiDBCollator::UTF8_GENERAL_CI);
        static const auto c_utf8mb4_general_ci = GeneralCICollator(ITiDBCollator::UTF8MB4_GENERAL_CI);
        static const auto c_utf8_unicode_ci = UCACICollator<Unicode0400, true>(ITiDBCollator::UTF8_UNICODE_CI);
        static const auto c_utf8mb4_unicode_ci = UCACICollator<Unicode0400, true>(ITiDBCollator::UTF8MB4_UNICODE_CI);
        static const auto c_utf8mb4_0900_ai_ci = UCACICollator<Unicode0900, false>(ITiDBCollator::UTF8MB4_0900_AI_CI);
        static const auto c_utf8mb4_0900_bin = UTF8MB4_0900_BIN_TYPE(ITiDBCollator::UTF8MB4_0900_BIN);
        static const auto c_utf8mb4_bin = UTF8MB4_BIN_TYPE(ITiDBCollator::UTF8MB4_BIN);
        static const auto c_latin1_bin = BinCollator<char, true>(ITiDBCollator::LATIN1_BIN);
        static const auto c_binary = BinCollator<char, false>(ITiDBCollator::BINARY);
        static const auto c_ascii_bin = BinCollator<char, true>(ITiDBCollator::ASCII_BIN);
        static const auto c_utf8_bin = UTF8MB4_BIN_TYPE(ITiDBCollator::UTF8_BIN);

#ifdef M
        static_assert(false, "`M` is defined");
#endif
#define M(name)                                               \
    do                                                        \
    {                                                         \
        auto & collator = (c_##name);                         \
        id_map[collator.getCollatorId()] = &collator;         \
        addr_to_type[&collator] = collator.getCollatorType(); \
        name_map[#name] = &collator;                          \
    } while (false)

        M(utf8_general_ci);
        M(utf8mb4_general_ci);
        M(utf8_unicode_ci);
        M(utf8mb4_unicode_ci);
        M(utf8mb4_0900_ai_ci);
        M(utf8mb4_0900_bin);
        M(utf8mb4_bin);
        M(latin1_bin);
        M(binary);
        M(ascii_bin);
        M(utf8_bin);
#undef M
    }
};

static const TiDBCollatorPtrMap tidb_collator_map;

TiDBCollatorPtr ITiDBCollator::getCollator(int32_t id)
{
    const auto & id_map = tidb_collator_map.id_map;
    if (auto it = id_map.find(id); it != id_map.end())
        return it->second;
    throw DB::Exception(
        fmt::format("{}: invalid collation ID: {}", __PRETTY_FUNCTION__, id),
        DB::ErrorCodes::LOGICAL_ERROR);
}

TiDBCollatorPtr ITiDBCollator::getCollator(const std::string & name)
{
    const auto & name_map = tidb_collator_map.name_map;
    if (auto it = name_map.find(Poco::toLower(name)); it != name_map.end())
        return it->second;
    return {};
}

bool ITiDBCollator::isBinary() const
{
    return collator_type == CollatorType::BINARY;
}
bool ITiDBCollator::isCI() const
{
    switch (collator_type)
    {
    case CollatorType::UTF8_UNICODE_CI:
    case CollatorType::UTF8_GENERAL_CI:
    case CollatorType::UTF8MB4_UNICODE_CI:
    case CollatorType::UTF8MB4_GENERAL_CI:
    case CollatorType::UTF8MB4_0900_AI_CI:
        return true;
    default:
        return false;
    }
}

ITiDBCollator::CollatorType GetTiDBCollatorType(const void * collator)
{
    const auto & addr_to_type = TiDB::tidb_collator_map.addr_to_type;
    if (auto it = addr_to_type.find(collator); it != addr_to_type.end())
        return it->second;
    return ITiDBCollator::CollatorType::MAX_;
}

} // namespace TiDB

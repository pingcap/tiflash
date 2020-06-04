#include <Common/Exception.h>
#include <Storages/Transaction/Collator.h>

#include <array>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiDB
{

std::string_view rtrim(const char * s, size_t length)
{
    auto v = std::string_view(s, length);
    size_t end = v.find_last_not_of(' ');
    return end == std::string_view::npos ? "" : v.substr(0, end + 1);
}

template <typename T>
int signum(T val)
{
    return (0 < val) - (val < 0);
}

template <typename Collator>
class Pattern : public ITiDBCollator::IPattern
{
public:
    void compile(const std::string & pattern, char escape) override
    {
        weights.clear();
        match_types.clear();

        weights.reserve(pattern.length() * sizeof(typename Collator::WeightType));
        match_types.reserve(pattern.length() * sizeof(typename Collator::WeightType));

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
                    auto old_offset = offset;
                    c = Collator::decodeChar(pattern.data(), old_offset);
                    if (c == escape || c == '_' || c == '%')
                        offset = old_offset;
                    else
                        c = escape;
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
            weights.push_back(Collator::weight(c));
            match_types.push_back(tp);
        }
    }

    bool match(const char * s, size_t length) const override
    {
        size_t s_offset = 0, next_s_offset = 0, tmp_s_offset = 0;
        size_t p_idx = 0, next_p_idx = 0;
        while (p_idx < weights.size() || s_offset < length)
        {
            if (p_idx < weights.size())
            {
                switch (match_types[p_idx])
                {
                    case Match:
                        if (s_offset < length && Collator::weight(Collator::decodeChar(s, tmp_s_offset = s_offset)) == weights[p_idx])
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
    std::vector<typename Collator::WeightType> weights;

    enum MatchType
    {
        Match,
        One,
        Any,
    };
    std::vector<MatchType> match_types;
};

template <bool padding = false>
class BinCollator : public ITiDBCollator
{
public:
    BinCollator(int32_t id) : ITiDBCollator(id) {}
    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        if constexpr (padding)
            return signum(rtrim(s1, length1).compare(rtrim(s2, length2)));
        else
            return signum(std::string_view(s1, length1).compare(std::string_view(s2, length2)));
    }

    StringRef sortKey(const char * s, size_t length, std::string &) const override
    {
        if constexpr (padding)
        {
            auto v = rtrim(s, length);
            return StringRef(v.data(), v.length());
        }
        else
        {
            return StringRef(s, length);
        }
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<BinCollator<padding>>>(); }

    const std::string & getLocale() const override { return name; }

private:
    const std::string name = padding ? "BinaryPadding" : "Binary";

private:
    static inline char decodeChar(const char * s, size_t & offset) { return s[offset++]; }

    using WeightType = char;
    static inline WeightType weight(char c) { return c; }

    friend class Pattern<BinCollator>;
};

namespace GeneralCI
{
using WeightType = uint16_t;
extern const std::array<WeightType, 256 * 256> weight_lut;
} // namespace GeneralCI

class GeneralCICollator : public ITiDBCollator
{
public:
    GeneralCICollator(int32_t id) : ITiDBCollator(id) {}

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
                return signum(cmp);
        }

        return (offset1 < v1.length()) - (offset2 < v2.length());
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        auto v = rtrim(s, length);
        if (length * sizeof(WeightType) > container.size())
            container.resize(length * sizeof(WeightType));
        size_t offset = 0;
        size_t total_size = 0;

        while (offset < v.length())
        {
            auto c = decodeChar(s, offset);
            auto sk = weight(c);
            container[total_size++] = char(sk >> 8);
            container[total_size++] = char(sk);
        }

        return StringRef(container.data(), total_size);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<GeneralCICollator>>(); }

    const std::string & getLocale() const override { return name; }

private:
    const std::string name = "GeneralCI";

private:
    using CharType = int32_t;
    using StringType = std::vector<CharType>;
    static constexpr uint8_t b2_mask = 0x1F;
    static constexpr uint8_t b3_mask = 0x0F;
    static constexpr uint8_t b4_mask = 0x07;
    static constexpr uint8_t mb_mask = 0x3F;
    static inline CharType decodeChar(const char * s, size_t & offset)
    {
        uint8_t b0 = s[offset];
        if (b0 < 0x80)
        {
            auto c = static_cast<CharType>(b0);
            offset += 1;
            return c;
        }
        if (b0 < 0xE0)
        {
            auto c = static_cast<CharType>(b0 & b2_mask) << 6 | static_cast<CharType>(s[1 + offset] & mb_mask);
            offset += 2;
            return c;
        }
        if (b0 < 0xF0)
        {
            auto c = static_cast<CharType>(b0 & b3_mask) << 12 | static_cast<CharType>(s[1 + offset] & mb_mask) << 6
                | static_cast<CharType>(s[2 + offset] & mb_mask);
            offset += 3;
            return c;
        }
        auto c = static_cast<CharType>(b0 & b4_mask) << 18 | static_cast<CharType>(s[1 + offset] & mb_mask) << 12
            | static_cast<CharType>(s[2 + offset] & mb_mask) << 6 | static_cast<CharType>(s[3 + offset] & mb_mask);
        offset += 4;
        return c;
    }

    using WeightType = GeneralCI::WeightType;
    static inline WeightType weight(CharType c)
    {
        if (c > 0xFFFF)
            return 0xFFFD;
        return GeneralCI::weight_lut[c & 0xFFFF];
        //return !!(c >> 16) * 0xFFFD + (1 - !!(c >> 16)) * GeneralCI::weight_lut[c & 0xFFFF];
    }

    friend class Pattern<GeneralCICollator>;
};

std::unique_ptr<ITiDBCollator> ITiDBCollator::getCollator(int32_t id)
{
    switch (id)
    {
        case ITiDBCollator::BINARY:
            return std::make_unique<BinCollator<false>>(id);
        case ITiDBCollator::ASCII_BIN:
        case ITiDBCollator::LATIN1_BIN:
        case ITiDBCollator::UTF8MB4_BIN:
        case ITiDBCollator::UTF8_BIN:
            return std::make_unique<BinCollator<true>>(id);
        case ITiDBCollator::UTF8_GENERAL_CI:
        case ITiDBCollator::UTF8MB4_GENERAL_CI:
            return std::make_unique<GeneralCICollator>(id);
        default:
            throw DB::Exception(
                std::string(__PRETTY_FUNCTION__) + ": invalid collation ID: " + std::to_string(id), DB::ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace TiDB

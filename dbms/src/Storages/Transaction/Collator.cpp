#include <Common/Exception.h>
#include <IO/Endian.h>
#include <Storages/Transaction/Collator.h>

#include <sstream>

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

        const auto & decoded = Collator::decode(pattern.data(), pattern.length());
        weights.reserve(decoded.size());
        match_types.reserve(decoded.size());
        bool last_any = false;
        for (size_t i = 0; i < decoded.size(); i++)
        {
            MatchType tp;
            auto c = decoded[i];
            if (c == escape)
            {
                last_any = false;
                tp = MatchType::Match;
                if (i < decoded.size() - 1)
                {
                    c = decoded[i + 1];
                    if (c == escape || c == '_' || c == '%')
                        i++;
                    else
                        c = escape;
                }
            }
            else if (c == '_')
            {
                if (last_any)
                    continue;
                tp = MatchType::One;
            }
            else if (c == '%')
            {
                if (last_any)
                    continue;
                last_any = true;
                tp = MatchType::Any;
            }
            else
            {
                last_any = false;
                tp = MatchType::Match;
            }
            weights.push_back(Collator::weight(c));
            match_types.push_back(tp);
        }
    }

    bool match(const char * s, size_t length) const override
    {
        const auto & decoded = Collator::decode(s, length);
        size_t s_idx = 0, p_idx = 0, next_s_idx = 0, next_p_idx = 0;
        while (p_idx < weights.size() || s_idx < decoded.size())
        {
            if (p_idx < weights.size())
            {
                switch (match_types[p_idx])
                {
                    case Match:
                        if (s_idx < decoded.size() && Collator::weight(decoded[s_idx]) == weights[p_idx])
                        {
                            p_idx++;
                            s_idx++;
                            continue;
                        }
                        break;
                    case One:
                        if (s_idx < decoded.size())
                        {
                            p_idx++;
                            s_idx++;
                            continue;
                        }
                        break;
                    case Any:
                        next_p_idx = p_idx;
                        next_s_idx = s_idx + 1;
                        p_idx++;
                        continue;
                }
            }
            if (0 < next_s_idx && next_s_idx <= decoded.size())
            {
                p_idx = next_p_idx;
                s_idx = next_s_idx;
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

    std::string sortKey(const char * s, size_t length) const override
    {
        if constexpr (padding)
            return std::string(rtrim(s, length));
        else
            return std::string(s, length);
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
    static inline std::string_view decode(const char * s, size_t length) { return std::string_view(s, length); }

    using WeightType = char;
    static inline WeightType weight(char c) { return c; }

    friend class Pattern<BinCollator>;
};

namespace GeneralCI
{
extern const uint16_t * weight_lut[];
}

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

        return signum((offset1 - v1.length()) - (offset2 - v2.length()));
    }

    std::string sortKey(const char * s, size_t length) const override
    {
        auto v = rtrim(s, length);
        std::stringbuf buf;

        size_t offset = 0;
        while (offset < v.length())
        {
            auto c = decodeChar(s, offset);
            auto sk = weight(c);
            buf.sputc(char(sk >> 8));
            buf.sputc(char(sk));
        }

        return buf.str();
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        auto v = rtrim(s, length);
        container.clear();
        size_t offset = 0;
        size_t total_size = 0;

#if __SSE2__
        WeightType wv[8];
        size_t wv_size = 0;

        while (offset < v.length())
        {
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));
            if (offset >= v.length())
                break;
            wv[wv_size++] = DB::toBigEndian(weight(decodeChar(s, offset)));

            memcpy(container.data() + total_size, &wv[0], wv_size * sizeof(WeightType));
            total_size += wv_size * sizeof(WeightType);
            wv_size = 0;
        }

        memcpy(container.data() + total_size, &wv[0], wv_size * sizeof(WeightType));
        total_size += wv_size * sizeof(WeightType);
#else
        while (offset < v.length())
        {
            auto c = decodeChar(s, offset);
            auto sk = weight(c);
            container[total_size++] = char(sk >> 8);
            container[total_size++] = char(sk);
        }
#endif

        return StringRef(container.data(), total_size);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<GeneralCICollator>>(); }

    const std::string & getLocale() const override { return name; }

private:
    const std::string name = "GeneralCI";

private:
    using CharType = int32_t;
    using StringType = std::vector<CharType>;
    static inline StringType decode(const char * s, size_t length)
    {
        StringType decoded;
        decoded.reserve(length);

        size_t offset = 0;
        while (offset < length)
        {
            auto c = decodeChar(s, offset);
            decoded.push_back(c);
        }
        return decoded;
    }

    using WeightType = uint16_t;
    static inline WeightType weight(CharType c)
    {
        if (c > 0xFFFF)
            return 0xFFFD;

        auto cell = GeneralCI::weight_lut[c >> 8][c & 0xFF];
        return (cell >> 8) == 0xFE ? uint16_t(c) : cell;
    }

    friend class Pattern<GeneralCICollator>;

private:
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

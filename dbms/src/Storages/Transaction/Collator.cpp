#include <Common/Exception.h>
#include <Storages/Transaction/Collator.h>
#include <unicode/uiter.h>

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

int signum(int val) { return (0 < val) - (val < 0); }

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

        UCharIterator iter1, iter2;
        uiter_setUTF8(&iter1, v1.data(), v1.length());
        uiter_setUTF8(&iter2, v2.data(), v2.length());
        auto c1 = uiter_next32(&iter1);
        auto c2 = uiter_next32(&iter2);
        while (c1 != U_SENTINEL && c2 != U_SENTINEL)
        {
            auto sk1 = weight(c1);
            auto sk2 = weight(c2);
            auto cmp = sk1 - sk2;
            if (cmp != 0)
                return signum(cmp);

            c1 = uiter_next32(&iter1);
            c2 = uiter_next32(&iter2);
        }

        return signum(c1 - c2);
    }

    std::string sortKey(const char * s, size_t length) const override
    {
        auto v = rtrim(s, length);
        std::stringbuf buf;

        UCharIterator iter;
        uiter_setUTF8(&iter, v.data(), v.length());
        auto c = uiter_next32(&iter);
        while (c != U_SENTINEL)
        {
            auto sk = weight(c);
            buf.sputc(char(sk >> 8));
            buf.sputc(char(sk));

            c = uiter_next32(&iter);
        }

        return buf.str();
    }

    StringRef sortKey(const char * s, size_t length, std::string & container) const override
    {
        auto v = rtrim(s, length);
        UCharIterator iter;
        uiter_setUTF8(&iter, v.data(), v.length());
        auto c = uiter_next32(&iter);
        size_t i = 0;
        while (c != U_SENTINEL)
        {
            auto sk = weight(c);
            container[i++] = char(sk >> 8);
            container[i++] = char(sk);
            c = uiter_next32(&iter);
        }

        return StringRef(container.data(), i);
    }

    std::unique_ptr<IPattern> pattern() const override { return std::make_unique<Pattern<GeneralCICollator>>(); }

    const std::string & getLocale() const override { return name; }

private:
    using CharType = UChar32;
    using StringType = std::vector<CharType>;
    const std::string name = "GeneralCI";
    static inline StringType decode(const char * s, size_t length)
    {
        StringType decoded;
        decoded.reserve(length);
        UCharIterator iter;
        uiter_setUTF8(&iter, s, length);
        auto c = uiter_next32(&iter);
        while (c != U_SENTINEL)
        {
            decoded.push_back(c);
            c = uiter_next32(&iter);
        }
        return decoded;
    }

    using WeightType = uint16_t;
    static WeightType weight(CharType c)
    {
        if (c > 0xFFFF)
            return 0xFFFD;

        auto plane = GeneralCI::weight_lut[c >> 8];
        if (plane == nullptr)
            return uint16_t(c);

        return plane[c & 0xFF];
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

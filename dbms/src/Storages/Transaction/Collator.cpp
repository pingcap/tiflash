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

class BinPattern : public ICollator::IPattern
{
public:
    void compile(const std::string & pattern, char escape) const override
    {
        // TODO: TBD.
        (void)pattern;
        (void)escape;
    }

    bool match(const char * s, size_t length) const override
    {
        // TODO: TBD.
        (void)s;
        (void)length;
        return false;
    }
};

class BinCollator : public ICollator
{
public:
    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        return signum(std::string_view(s1, length1).compare(std::string_view(s2, length2)));
    }

    std::string sortKey(const char * s, size_t length) const override { return std::string(s, length); }

    std::unique_ptr<IPattern> pattern() const override { return std::unique_ptr<BinPattern>(); }
};

class BinPaddingCollator : public ICollator
{
public:
    int compare(const char * s1, size_t length1, const char * s2, size_t length2) const override
    {
        return signum(rtrim(s1, length1).compare(rtrim(s2, length2)));
    }

    std::string sortKey(const char * s, size_t length) const override { return std::string(rtrim(s, length)); }

    std::unique_ptr<IPattern> pattern() const override { return std::unique_ptr<BinPattern>(); }
};

namespace GeneralCI
{
extern const uint16_t * sk_lut[];
}

class GeneralCICollator : public ICollator
{
public:
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
            auto sk1 = lookupSortKey(c1);
            auto sk2 = lookupSortKey(c2);
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
            auto sk = lookupSortKey(c);
            buf.sputc(char(sk >> 8));
            buf.sputc(char(sk));

            c = uiter_next32(&iter);
        }

        return buf.str();
    }

    std::unique_ptr<IPattern> pattern() const override { return std::unique_ptr<BinPattern>(); }

private:
    uint16_t lookupSortKey(UChar32 c) const
    {
        if (c > 0xFFFF)
            return 0xFFFD;

        auto plane = GeneralCI::sk_lut[c >> 8];
        if (plane == nullptr)
            return uint16_t(c);

        return plane[c & 0xFF];
    }
};

std::unique_ptr<ICollator> ICollator::getCollator(int32_t id)
{
    switch (id)
    {
        case ICollator::BINARY:
            return std::make_unique<BinCollator>();
        case ICollator::ASCII_BIN:
        case ICollator::LATIN1_BIN:
        case ICollator::UTF8MB4_BIN:
        case ICollator::UTF8_BIN:
            return std::make_unique<BinPaddingCollator>();
        case ICollator::UTF8_GENERAL_CI:
        case ICollator::UTF8MB4_GENERAL_CI:
            return std::make_unique<GeneralCICollator>();
        default:
            throw DB::Exception(
                std::string(__PRETTY_FUNCTION__) + ": invalid collation ID: " + std::to_string(id), DB::ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace TiDB

#pragma once

#include <boost/noncopyable.hpp>
#include <memory>

namespace TiDB
{

class ICollator
{
public:
    enum
    {
        UTF8_GENERAL_CI = 33,
        UTF8MB4_GENERAL_CI = 45,
        UTF8MB4_BIN = 46,
        LATIN1_BIN = 47,
        BINARY = 63,
        ASCII_BIN = 65,
        UTF8_BIN = 83,
    };

    /// Get the collator according to the internal collation ID, which directly comes from tipb and has been properly
    /// de-rewritten - the "New CI Collation" will flip the sign of the collation ID.
    static std::unique_ptr<ICollator> getCollator(int32_t id);

    class IPattern
    {
    public:
        virtual ~IPattern() = default;

        virtual void compile(const std::string & pattern, char escape) const = 0;
        virtual bool match(const char * s, size_t length) const = 0;

    protected:
        IPattern() = default;
    };

    virtual ~ICollator() = default;

    virtual int compare(const char * s1, size_t length1, const char * s2, size_t length2) const = 0;
    virtual std::string sortKey(const char * s, size_t length) const = 0;
    virtual std::unique_ptr<IPattern> pattern() const = 0;

protected:
    ICollator() = default;
};

} // namespace TiDB
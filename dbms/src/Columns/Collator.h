#pragma once

#include <memory>
#include <string>
#include <boost/noncopyable.hpp>

struct UCollator;

class ICollator : private boost::noncopyable
{
public:
    ICollator() = default;

    virtual ~ICollator() = default;

    virtual int compare(const char * str1, size_t length1, const char * str2, size_t length2) const = 0;

    virtual const std::string & getLocale() const = 0;
};

class Collator : public ICollator
{
public:
    explicit Collator(const std::string & locale_);

    ~Collator() override;

    int compare(const char * str1, size_t length1, const char * str2, size_t length2) const override;

    const std::string & getLocale() const override;

private:
    std::string locale;
    UCollator * collator;
};


using ICollatorPtr = std::shared_ptr<ICollator>;
using CollatorPtr = std::shared_ptr<Collator>;

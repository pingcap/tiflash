#pragma once

#include <Storages/Transaction/SerializationHelper.h>
#include <Storages/Transaction/TiKVDecodedValue.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

template <bool is_key>
struct StringObject : std::string
{
public:
    using Base = std::string;

    struct Hash
    {
        std::size_t operator()(const StringObject & x) const { return std::hash<Base>()(x); }
    };

    StringObject() = default;
    StringObject(Base && str_) : Base(std::move(str_)) {}
    StringObject(StringObject && obj) : Base((Base &&) obj) {}
    StringObject(const char * str, const size_t len) : Base(str, len) {}
    StringObject(const char * str) : Base(str) {}
    static StringObject copyFrom(const Base & str) { return StringObject(str); }

    StringObject & operator=(const StringObject & a) = delete;
    StringObject & operator=(StringObject && a)
    {
        if (this == &a)
            return *this;

        (Base &)* this = (Base &&) a;
        return *this;
    }

    const std::string & getStr() const { return *this; }
    size_t dataSize() const { return Base::size(); }
    std::string toString() const { return *this; }

    // For debug
    std::string toHex() const
    {
        std::stringstream ss;
        ss << "[" << std::hex;
        for (size_t i = 0; i < dataSize(); ++i)
        {
            ss << Int32(UInt8(at(i)));
            if (i + 1 != dataSize())
                ss << ' ';
        }
        ss << "]";
        return ss.str();
    }

    explicit operator bool() const { return !empty(); }

    size_t serialize(WriteBuffer & buf) const { return writeBinary2((const Base &)*this, buf); }

    static StringObject deserialize(ReadBuffer & buf) { return StringObject(readBinary2<Base>(buf)); }

    ValueExtraInfo<is_key> & extraInfo() const { return extra; }

private:
    StringObject(const Base & str_) : Base(str_) {}
    StringObject(const StringObject & obj) = delete;
    size_t size() const = delete;

private:
    mutable ValueExtraInfo<is_key> extra;
};

using TiKVKey = StringObject<true>;
using TiKVValue = StringObject<false>;
using TiKVKeyValue = std::pair<TiKVKey, TiKVValue>;

struct DecodedTiKVKey : std::string, private boost::noncopyable
{
    using Base = std::string;
    DecodedTiKVKey(Base && str_) : Base(std::move(str_)) {}
    DecodedTiKVKey() = default;
    DecodedTiKVKey(DecodedTiKVKey && obj) : Base((Base &&) obj) {}
    DecodedTiKVKey & operator=(DecodedTiKVKey && obj)
    {
        if (this == &obj)
            return *this;

        (Base &)* this = (Base &&) obj;
        return *this;
    }
};

static_assert(sizeof(DecodedTiKVKey) == sizeof(std::string));

} // namespace DB

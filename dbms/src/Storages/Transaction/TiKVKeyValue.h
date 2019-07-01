#pragma once

#include <Storages/Transaction/SerializationHelper.h>
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
    explicit StringObject(Base && str_) : Base(std::move(str_)) {}
    explicit StringObject(const Base & str_) : Base(str_) {}
    StringObject(StringObject && obj) : Base((Base &&) obj) {}
    StringObject(const StringObject & obj) : Base(obj) {}
    StringObject & operator=(const StringObject & a)
    {
        if (this == &a)
            return *this;

        (Base &)* this = (const Base &)a;
        return *this;
    }
    StringObject & operator=(StringObject && a)
    {
        (Base &)* this = (Base &&) a;
        return *this;
    }

    const std::string & getStr() const { return *this; }
    std::string & getStrMut() { return *this; }
    size_t dataSize() const { return size(); }
    std::string toString() const { return *this; }

    String prefixNext() const {
        String new_str = *this;
        for (int i = int(size()) ; i >= 0 ; i--) {
            char & c = new_str[i-1];
            c ++;
            if (c != 0) {
                return new_str;
            }
        }
        return (*this) + "\0";
    }

    // For debug
    std::string toHex() const
    {
        std::stringstream ss;
        ss << dataSize() << "[" << std::hex;
        for (size_t i = 0; i < dataSize(); ++i)
            ss << at(i) << ((i + 1 == dataSize()) ? "" : " ");
        ss << "]";
        return ss.str();
    }

    explicit operator bool() const { return !empty(); }

    size_t serialize(WriteBuffer & buf) const { return writeBinary2((const Base &)*this, buf); }

    static StringObject deserialize(ReadBuffer & buf) { return StringObject(readBinary2<Base>(buf)); }
};

using TiKVKey = StringObject<true>;
using TiKVValue = StringObject<false>;
using TiKVKeyValue = std::pair<TiKVKey, TiKVValue>;

static_assert(sizeof(TiKVKey) == sizeof(std::string));
static_assert(sizeof(TiKVValue) == sizeof(std::string));

} // namespace DB

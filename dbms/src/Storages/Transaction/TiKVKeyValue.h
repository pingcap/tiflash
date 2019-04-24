#pragma once

#include <Storages/Transaction/SerializationHelper.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

template <bool is_key>
struct StringObject
{
public:
    using T = StringObject<is_key>;

    struct Hash
    {
        std::size_t operator()(const T & x) const { return std::hash<String>()(x.str); }
    };

    StringObject() = default;
    explicit StringObject(String && str_) : str(std::move(str_)) {}
    explicit StringObject(const String & str_) : str(str_) {}
    StringObject(StringObject && obj) : str(std::move(obj.str)) {}
    StringObject(const StringObject & obj) : str(obj.str) {}
    StringObject & operator=(const StringObject & a)
    {
        if (this == &a)
            return *this;

        str = a.str;
        return *this;
    }
    StringObject & operator=(StringObject && a)
    {
        str = std::move(a.str);
        return *this;
    }

    const String & getStr() const { return str; }
    String & getStrRef() { return str; }
    const char * data() const { return str.data(); }
    size_t dataSize() const { return str.size(); }

    String toString() const { return str; }

    // For debug
    String toHex() const
    {
        std::stringstream ss;
        ss << str.size() << "[" << std::hex;
        for (size_t i = 0; i < str.size(); ++i)
            ss << str.at(i) << ((i + 1 == str.size()) ? "" : " ");
        ss << "]";
        return ss.str();
    }

    bool empty() const { return str.empty(); }
    explicit operator bool() const { return !str.empty(); }
    bool operator==(const T & rhs) const { return str == rhs.str; }
    bool operator!=(const T & rhs) const { return str != rhs.str; }
    bool operator<(const T & rhs) const { return str < rhs.str; }
    bool operator<=(const T & rhs) const { return str <= rhs.str; }
    bool operator>(const T & rhs) const { return str > rhs.str; }
    bool operator>=(const T & rhs) const { return str >= rhs.str; }
    void operator+=(const String & s) { str += s; }

    size_t serialize(WriteBuffer & buf) const { return writeBinary2(str, buf); }

    static T deserialize(ReadBuffer & buf) { return T(readBinary2<String>(buf)); }

private:
    String str;
};

using TiKVKey = StringObject<true>;
using TiKVValue = StringObject<false>;
using TiKVKeyValue = std::pair<TiKVKey, TiKVValue>;

} // namespace DB

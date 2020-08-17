#pragma once

#include <Storages/Transaction/AtomicDecodedRow.h>
#include <Storages/Transaction/SerializationHelper.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

inline std::string ToHex(const char * data, const size_t size)
{
    std::stringstream ss;
    ss << "[" << std::hex;
    for (size_t i = 0; i < size; ++i)
    {
        ss << Int32(UInt8(data[i]));
        if (i + 1 != size)
            ss << ' ';
    }
    ss << "]";
    return ss.str();
}

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
    std::string toHex() const { return ToHex(data(), dataSize()); }

    explicit operator bool() const { return !empty(); }

    size_t serialize(WriteBuffer & buf) const { return writeBinary2((const Base &)*this, buf); }

    static StringObject deserialize(ReadBuffer & buf) { return StringObject(readBinary2<Base>(buf)); }

    AtomicDecodedRow<is_key> & getDecodedRow() const { return decoded_row; }

private:
    StringObject(const Base & str_) : Base(str_) {}
    StringObject(const StringObject & obj) = delete;
    size_t size() const = delete;

private:
    mutable AtomicDecodedRow<is_key> decoded_row;
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

struct RawTiDBPK : std::shared_ptr<const std::string>
{
    using Base = std::shared_ptr<const std::string>;

    struct Hash
    {
        size_t operator()(const RawTiDBPK & x) const { return std::hash<std::string>()(*x); }
    };

    bool operator==(const RawTiDBPK & y) const { return (**this) == (*y); }
    bool operator!=(const RawTiDBPK & y) const { return !((*this) == y); }
    bool operator<(const RawTiDBPK & y) const { return (**this) < (*y); }

    RawTiDBPK(const Base & o) : Base(o), handle(o->size() == 8 ? getHandleID() : 0) {}

    std::string toHex() const
    {
        auto & p = *this;
        return ToHex(p->data(), p->size());
    }

    // Make this struct can be casted into HandleID implicitly.
    operator HandleID() const { return handle; }

private:
    HandleID getHandleID() const;

private:
    const HandleID handle;
};

} // namespace DB

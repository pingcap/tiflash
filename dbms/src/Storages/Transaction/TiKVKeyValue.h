// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/RedactHelpers.h>
#include <Common/nocopyable.h>
#include <Storages/Transaction/SerializationHelper.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
static const size_t KEYSPACE_PREFIX_LEN = 4;
static const char TXN_MODE_PREFIX = 'x';

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
    StringObject(Base && str_)
        : Base(std::move(str_))
    {}
    StringObject(StringObject && obj)
        : Base((Base &&) obj)
    {}
    StringObject(const char * str, const size_t len)
        : Base(str, len)
    {}
    StringObject(const char * str)
        : Base(str)
    {}
    static StringObject copyFrom(const Base & str) { return StringObject(str); }

    DISALLOW_COPY(StringObject);
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

    // Format as a hex string for debugging. The value will be converted to '?' if redact-log is on
    std::string toDebugString() const { return Redact::keyToDebugString(data(), dataSize()); }

    explicit operator bool() const { return !empty(); }

    size_t serialize(WriteBuffer & buf) const { return writeBinary2((const Base &)*this, buf); }

    static StringObject deserialize(ReadBuffer & buf) { return StringObject(readBinary2<Base>(buf)); }

private:
    StringObject(const Base & str_)
        : Base(str_)
    {}
    size_t size() const = delete;
};

using TiKVKey = StringObject<true>;
using TiKVValue = StringObject<false>;
using TiKVKeyValue = std::pair<TiKVKey, TiKVValue>;

struct DecodedTiKVKey : std::string
    , private boost::noncopyable
{
    using Base = std::string;
    DecodedTiKVKey(Base && str_)
        : Base(std::move(str_))
    {}
    DecodedTiKVKey() = default;
    DecodedTiKVKey(DecodedTiKVKey && obj)
        : Base((Base &&) obj)
    {}
    DecodedTiKVKey & operator=(DecodedTiKVKey && obj)
    {
        if (this == &obj)
            return *this;

        (Base &)* this = (Base &&) obj;
        return *this;
    }

    KeyspaceID getKeyspaceID() const;
    std::string_view getUserKey() const;
    static std::string makeKeyspacePrefix(KeyspaceID keyspace_id);
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

    RawTiDBPK(const Base & o)
        : Base(o)
        , handle(o->size() == 8 ? getHandleID() : 0)
    {}

    // Format as a hex string for debugging. The value will be converted to '?' if redact-log is on
    std::string toDebugString() const
    {
        auto & p = *this;
        return Redact::keyToDebugString(p->data(), p->size());
    }

    // Make this struct can be casted into HandleID implicitly.
    operator HandleID() const { return handle; }

private:
    HandleID getHandleID() const;

private:
    HandleID handle;
};

} // namespace DB

// Copyright 2023 PingCAP, Inc.
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

#include <Storages/KVStore/TiKVHelpers/TiKVKeyValue.h>
#include <Storages/KVStore/Types.h>

namespace DB
{
struct DecodedTiKVKey;
using DecodedTiKVKeyPtr = std::shared_ptr<DecodedTiKVKey>;

struct DecodedTiKVKey
    : std::string
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

    static DecodedTiKVKey copyFrom(const DecodedTiKVKey & other) { return DecodedTiKVKey(other.toBase()); }

    KeyspaceID getKeyspaceID() const;
    std::string_view getUserKey() const;
    // Format as a hex string for debugging. The value will be converted to '?' if redact-log is on
    std::string toDebugString() const;
    std::string toString() const { return toBase(); }
    static std::string makeKeyspacePrefix(KeyspaceID keyspace_id);

private:
    Base toBase() const { return *this; }
};

static_assert(sizeof(DecodedTiKVKey) == sizeof(std::string));

struct RawTiDBPK : private std::shared_ptr<const std::string>
{
    using Base = std::shared_ptr<const std::string>;

    struct Hash
    {
        size_t operator()(const RawTiDBPK & x) const { return std::hash<std::string>()(*x); }
    };

    bool operator==(const RawTiDBPK & y) const { return (**this) == (*y); }
    bool operator!=(const RawTiDBPK & y) const { return !((*this) == y); }
    bool operator<(const RawTiDBPK & y) const { return (**this) < (*y); }
    bool operator>(const RawTiDBPK & y) const { return (**this) > (*y); }
    const std::string * operator->() const { return get(); }
    const std::string & operator*() const { return *get(); }

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

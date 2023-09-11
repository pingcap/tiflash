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


#include <Common/RedactHelpers.h>
#include <Common/nocopyable.h>
#include <Storages/KVStore/Utils/SerializationHelper.h>

namespace DB
{
template <bool is_key>
struct StringObject : std::string
{
public:
    using Base = std::string;

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

    template <bool a, typename = std::enable_if_t<a == is_key>>
    static StringObject<is_key> copyFromObj(const StringObject<a> & other)
    {
        return StringObject(other.data(), other.dataSize());
    }

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
} // namespace DB

namespace std
{
template <bool is_key>
struct hash<DB::StringObject<is_key>>
{
    size_t operator()(const DB::StringObject<is_key> & k) const { return std::hash<std::string>()(k); }
};
} // namespace std
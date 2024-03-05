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

#include <Common/RedactHelpers.h>
#include <Common/hex.h>
#include <IO/WriteHelpers.h>
#include <common/types.h>
#include <pingcap/RedactHelpers.h>

#include <iomanip>

std::atomic<bool> Redact::REDACT_LOG = false;

void Redact::setRedactLog(bool v)
{
    pingcap::Redact::setRedactLog(v); // set redact flag for client-c
    Redact::REDACT_LOG.store(v, std::memory_order_relaxed);
}

std::string Redact::handleToDebugString(int64_t handle)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
        return "?";

    // Encode as string
    return DB::toString(handle);
}

std::string Redact::keyToHexString(const char * key, size_t size)
{
    // Encode as upper hex string
    std::string buf(size * 2, '\0');
    char * pos = buf.data();
    for (size_t i = 0; i < size; ++i)
    {
        writeHexByteUppercase(static_cast<UInt8>(key[i]), pos);
        pos += 2;
    }
    return buf;
}

std::string Redact::keyToDebugString(const char * key, const size_t size)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
        return "?";

    return Redact::keyToHexString(key, size);
}

void Redact::keyToDebugString(const char * key, const size_t size, std::ostream & oss)
{
    if (Redact::REDACT_LOG.load(std::memory_order_relaxed))
    {
        oss << "?";
        return;
    }

    // Encode as upper hex string
    const auto flags = oss.flags();
    oss << std::uppercase << std::setfill('0') << std::hex;
    for (size_t i = 0; i < size; ++i)
    {
        // width need to be set for each output (https://stackoverflow.com/questions/405039/permanent-stdsetw)
        oss << std::setw(2) << static_cast<Int32>(static_cast<UInt8>(key[i]));
    }
    oss.flags(flags); // restore flags
}

std::string Redact::hexStringToKey(const char * start, size_t len)
{
    std::string s;
    if (len & 1)
        throw DB::Exception("Invalid length: " + std::string(start, len), DB::ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < len; i += 2)
    {
        int x;
        std::stringstream ss;
        ss << std::hex << std::string(start + i, start + i + 2);
        ss >> x;
        s.push_back(x);
    }
    return s;
}
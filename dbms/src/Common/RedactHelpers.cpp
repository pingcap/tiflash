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

#include <Common/FmtUtils.h>
#include <Common/RedactHelpers.h>
#include <Common/hex.h>
#include <IO/WriteHelpers.h>
#include <common/types.h>
#include <pingcap/RedactHelpers.h>

#include <iomanip>
#include <string>

std::atomic<RedactMode> Redact::REDACT_LOG = RedactMode::Disable;

void Redact::setRedactLog(RedactMode v)
{
    switch (v)
    {
    case RedactMode::Enable:
        pingcap::Redact::setRedactLog(pingcap::RedactMode::Enable);
    case RedactMode::Disable:
        pingcap::Redact::setRedactLog(pingcap::RedactMode::Disable);
    case RedactMode::Marker:
        pingcap::Redact::setRedactLog(pingcap::RedactMode::Marker);
    }
    Redact::REDACT_LOG.store(v, std::memory_order_relaxed);
}

std::string Redact::toMarkerString(const std::string & raw, bool ignore_escape)
{
    if (likely(ignore_escape))
        return fmt::format("‹{}›", raw);

    constexpr static size_t LT_SIZE = std::string_view("‹").size();
    constexpr static size_t GT_SIZE = std::string_view("›").size();
    constexpr static int LT_TYPE = 1;
    constexpr static int GT_TYPE = 2;
    std::map<size_t, int> found_pos;
    std::string::size_type pos = 0;
    do
    {
        pos = raw.find("‹", pos);
        if (pos == std::string::npos)
            break;
        found_pos.emplace(pos, LT_TYPE);
        pos += LT_SIZE;
    } while (pos != std::string::npos);
    pos = 0;
    do
    {
        pos = raw.find("›", pos);
        if (pos == std::string::npos)
            break;
        found_pos.emplace(pos, GT_TYPE);
        pos += GT_SIZE;
    } while (pos != std::string::npos);
    if (likely(found_pos.empty()))
    {
        // nothing to be escaped
        return fmt::format("‹{}›", raw);
    }

    DB::FmtBuffer fmt_buf;
    fmt_buf.append("‹");
    pos = 0;
    for (const auto & [to_escape_pos, to_escape_type] : found_pos)
    {
        if (to_escape_type == LT_TYPE)
        {
            fmt_buf.append(std::string_view(raw.c_str() + pos, to_escape_pos - pos + LT_SIZE));
            fmt_buf.append("‹");
            pos = to_escape_pos + LT_SIZE;
        }
        else if (to_escape_type == GT_TYPE)
        {
            fmt_buf.append(std::string_view(raw.c_str() + pos, to_escape_pos - pos + GT_SIZE));
            fmt_buf.append("›");
            pos = to_escape_pos + GT_SIZE;
        }
    }
    fmt_buf.append("›");
    return fmt_buf.toString();
}

std::string Redact::handleToDebugString(int64_t handle)
{
    const auto v = Redact::REDACT_LOG.load(std::memory_order_relaxed);
    switch (v)
    {
    case RedactMode::Enable:
        return "?";
    case RedactMode::Disable:
        // Encode as string
        return DB::toString(handle);
    case RedactMode::Marker:
        // Note: the `handle` must be int64 so we don't need to care
        // about escaping here.
        return toMarkerString(DB::toString(handle), /*ignore_escape*/ true);
    }
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
    const auto v = Redact::REDACT_LOG.load(std::memory_order_relaxed);
    switch (v)
    {
    case RedactMode::Enable:
        return "?";
    case RedactMode::Disable:
        // Encode as string
        return Redact::keyToHexString(key, size);
    case RedactMode::Marker:
        // Note: the `s` must be hexadecimal string so we don't need to care
        // about escaping here.
        return toMarkerString(Redact::keyToHexString(key, size), /*ignore_escape*/ true);
    }
}

void Redact::keyToDebugString(const char * key, const size_t size, std::ostream & oss)
{
    const auto v = Redact::REDACT_LOG.load(std::memory_order_relaxed);
    switch (v)
    {
    case RedactMode::Enable:
    {
        oss << "?";
        return;
    }
    case RedactMode::Disable:
    {
        oss << Redact::keyToHexString(key, size);
        return;
    }
    case RedactMode::Marker:
    {
        // Note: the `s` must be hexadecimal string so we don't need to care
        // about escaping here.
        oss << toMarkerString(Redact::keyToHexString(key, size), /*ignore_escape*/ true);
        return;
    }
    }
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

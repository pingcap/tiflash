// Copyright 2024 PingCAP, Ltd.
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

#include <Storages/KVStore/MultiRaft/Disagg/ServerlessUtils.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>

namespace DB
{
// make_keyspace_prefix
String makeKeyspacePrefix(uint32_t keyspace_id, uint8_t suffix)
{
    WriteBufferFromOwnString buff;
    writeChar(UniversalPageIdFormat::KV_PREFIX, buff);
    // pub const KEYSPACE_META_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, KEYSPACE_META_PREFIX];
    // pub const KEYSPACE_META_PREFIX: u8 = 0x04;
    writeChar(0x01, buff);
    writeChar(0x04, buff);
    UniversalPageIdFormat::encodeUInt32(keyspace_id, buff);
    writeChar(suffix, buff);
    return buff.releaseStr();
}

// make_region_prefix
String makeRegionPrefix(uint64_t region_id, uint8_t suffix)
{
    WriteBufferFromOwnString buff;
    writeChar(UniversalPageIdFormat::KV_PREFIX, buff);
    // pub const REGION_RAFT_PREFIX: u8 = 0x02;
    // pub const REGION_RAFT_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_RAFT_PREFIX];
    writeChar(0x01, buff);
    writeChar(0x02, buff);
    UniversalPageIdFormat::encodeUInt64(region_id, buff);
    writeChar(suffix, buff);
    return buff.releaseStr();
}

#if SERVERLESS_PROXY == 0
// pub const KEYSPACE_INNER_KEY_OFF_SUFFIX: u8 = 0x01;
String getKeyspaceInnerKey(UniversalPageStoragePtr, uint32_t)
{
    return "";
}

// pub const REGION_INNER_KEY_OFF_SUFFIX: u8 = 0x06;
String getRegionInnerKey(UniversalPageStoragePtr, uint64_t)
{
    return "";
}

String getCompactibleInnerKey(UniversalPageStoragePtr, uint32_t, uint64_t)
{
    return "";
}

// pub const REGION_ENCRYPTION_KEY_SUFFIX: u8 = 0x07;
String getRegionEncKey(UniversalPageStoragePtr, uint64_t)
{
    return "";
}

// pub const KEYSPACE_ENCRYPTION_KEY_SUFFIX: u8 = 0x02;
String getKeyspaceEncKey(UniversalPageStoragePtr, uint32_t)
{
    return "";
}

String getCompactibleEncKey(UniversalPageStoragePtr, uint32_t, uint64_t)
{
    return "";
}

// pub const REGION_META_VERSION_SUFFIX: u8 = 0x09;
UInt64 getShardVer(UniversalPageStoragePtr, uint64_t)
{
    return 0;
}

// pub const REGION_TXN_FILE_LOCKS_SUFFIX: u8 = 0x08;
String getTxnFileRef(UniversalPageStoragePtr, uint64_t)
{
    return "";
}
#else
// pub const KEYSPACE_INNER_KEY_OFF_SUFFIX: u8 = 0x01;
String getKeyspaceInnerKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id)
{
    auto key = makeKeyspacePrefix(keyspace_id, 0x01);
    try
    {
        auto page = uni_ps->read(key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            return String(page.data.begin(), page.data.size());
        }
        else
        {
            return "";
        }
    }
    catch (...)
    {
        return "";
    }
}

// pub const REGION_INNER_KEY_OFF_SUFFIX: u8 = 0x06;
String getRegionInnerKey(UniversalPageStoragePtr uni_ps, uint64_t region_id)
{
    auto key = makeRegionPrefix(region_id, 0x06);
    try
    {
        auto page = uni_ps->read(key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            return String(page.data.begin(), page.data.size());
        }
        else
        {
            return "";
        }
    }
    catch (...)
    {
        return "";
    }
}

String getCompactibleInnerKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id, uint64_t region_id)
{
    auto keyspace = getKeyspaceInnerKey(uni_ps, keyspace_id);
    if unlikely (keyspace.empty())
    {
        auto region = getRegionInnerKey(uni_ps, region_id);
        if unlikely (region.empty())
        {
            LOG_INFO(
                DB::Logger::get(),
                "Failed to find compactible inner key, region_id={}, keyspace_id={}",
                region_id,
                keyspace_id);
        }
        return region;
    }
    else
    {
        return keyspace;
    }
}

// pub const REGION_ENCRYPTION_KEY_SUFFIX: u8 = 0x07;
String getRegionEncKey(UniversalPageStoragePtr uni_ps, uint64_t region_id)
{
    auto key = makeRegionPrefix(region_id, 0x07);
    try
    {
        auto page = uni_ps->read(key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            return String(page.data.begin(), page.data.size());
        }
        else
        {
            return "";
        }
    }
    catch (...)
    {
        return "";
    }
}

// pub const KEYSPACE_ENCRYPTION_KEY_SUFFIX: u8 = 0x02;
String getKeyspaceEncKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id)
{
    auto key = makeKeyspacePrefix(keyspace_id, 0x02);
    try
    {
        auto page = uni_ps->read(key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            return String(page.data.begin(), page.data.size());
        }
        else
        {
            return "";
        }
    }
    catch (...)
    {
        return "";
    }
}

String getCompactibleEncKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id, uint64_t region_id)
{
    auto keyspace = getKeyspaceEncKey(uni_ps, keyspace_id);
    if unlikely (keyspace.empty())
    {
        auto region = getRegionEncKey(uni_ps, region_id);
        if unlikely (region.empty())
        {
            LOG_INFO(
                DB::Logger::get(),
                "Failed to find compactible enc key, region_id={}, keyspace_id={}",
                region_id,
                keyspace_id);
        }
        return region;
    }
    else
    {
        return keyspace;
    }
}

// pub const REGION_META_VERSION_SUFFIX: u8 = 0x09;
UInt64 getShardVer(UniversalPageStoragePtr uni_ps, uint64_t region_id)
{
    auto key = makeRegionPrefix(region_id, 0x09);
    try
    {
        auto page = uni_ps->read(key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            return UniversalPageIdFormat::decodeUInt64(page.data.data());
        }
        else
        {
            LOG_INFO(DB::Logger::get(), "Failed to find shard_ver, region_id={}, key={}", region_id, key);
            return 0;
        }
    }
    catch (...)
    {
        LOG_INFO(
            DB::Logger::get(),
            "Failed to find shard_ver, region_id={}, key={}",
            region_id,
            Redact::keyToHexString(key.data(), key.size()));
        return 0;
    }
}

// pub const REGION_TXN_FILE_LOCKS_SUFFIX: u8 = 0x08;
String getTxnFileRef(UniversalPageStoragePtr uni_ps, uint64_t region_id)
{
    auto key = makeRegionPrefix(region_id, 0x08);
    try
    {
        auto page = uni_ps->read(key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            return String(page.data.begin(), page.data.size());
        }
        else
        {
            LOG_INFO(DB::Logger::get(), "Failed to find txn ref, region_id={}, key={}", region_id, key);
            return "";
        }
    }
    catch (...)
    {
        LOG_INFO(
            DB::Logger::get(),
            "Failed to find txn ref, region_id={}, key={}",
            region_id,
            Redact::keyToHexString(key.data(), key.size()));
        return "";
    }
}
#endif

} // namespace DB

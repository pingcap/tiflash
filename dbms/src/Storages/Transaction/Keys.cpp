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

#include "Keys.h"

#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{
namespace keys
{
void makeRegionPrefix(WriteBuffer & ss, uint64_t region_id, uint8_t suffix)
{
    uint8_t kk1 = LOCAL_PREFIX;
    uint8_t kk2 = REGION_RAFT_PREFIX;
    ss.write(reinterpret_cast<const char *>(&kk1), 1);
    ss.write(reinterpret_cast<const char *>(&kk2), 1);
    auto encoded = RecordKVFormat::encodeUInt64(region_id);
    decltype(auto) src = reinterpret_cast<const char *>(&encoded);
    ss.write(src, sizeof encoded);
    ss.write(reinterpret_cast<const char *>(&suffix), 1);
}

void makeRegionMeta(WriteBuffer & ss, uint64_t region_id, uint8_t suffix)
{
    uint8_t kk1 = LOCAL_PREFIX;
    uint8_t kk2 = REGION_META_PREFIX;
    ss.write(reinterpret_cast<const char *>(&kk1), 1);
    ss.write(reinterpret_cast<const char *>(&kk2), 1);
    auto encoded = RecordKVFormat::encodeUInt64(region_id);
    decltype(auto) src = reinterpret_cast<const char *>(&encoded);
    ss.write(src, sizeof encoded);
    ss.write(reinterpret_cast<const char *>(&suffix), 1);
}

std::string regionStateKey(uint64_t region_id)
{
    WriteBufferFromOwnString buff;
    makeRegionMeta(buff, region_id, REGION_STATE_SUFFIX);
    return buff.releaseStr();
}

std::string applyStateKey(uint64_t region_id)
{
    WriteBufferFromOwnString buff;
    makeRegionPrefix(buff, region_id, APPLY_STATE_SUFFIX);
    return buff.releaseStr();
}

bool validateRegionStateKey(const char * buf, size_t len, uint64_t region_id)
{
    if (len != 11)
        return false;
    if (buf[0] != LOCAL_PREFIX)
        return false;
    if (buf[1] != REGION_META_PREFIX)
        return false;
    auto parsed_region_id = RecordKVFormat::decodeUInt64(*reinterpret_cast<const UInt64 *>(buf + 2));
    if (buf[10] != REGION_STATE_SUFFIX)
        return false;
    if (region_id != parsed_region_id)
        return false;
    return true;
}

bool validateApplyStateKey(const char * buf, size_t len, uint64_t region_id)
{
    if (len != 11)
        return false;
    if (buf[0] != LOCAL_PREFIX)
        return false;
    if (buf[1] != REGION_RAFT_PREFIX)
        return false;
    auto parsed_region_id = RecordKVFormat::decodeUInt64(*reinterpret_cast<const UInt64 *>(buf + 2));
    if (buf[10] != APPLY_STATE_SUFFIX)
        return false;
    if (region_id != parsed_region_id)
        return false;
    return true;
}

} // namespace keys
} // namespace DB
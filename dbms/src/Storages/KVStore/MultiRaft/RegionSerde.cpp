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

#include <Common/FailPoint.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/Utils/SerializationHelper.h>

#include <memory>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char force_region_persist_version[];
extern const char force_region_read_version[];
extern const char force_region_persist_extension_field[];
extern const char force_region_read_extension_field[];
} // namespace FailPoints

enum class RegionPersistVersion
{
    V1 = 1,
    V2, // For eager gc
};

namespace RegionPersistFormat
{
static constexpr UInt32 HAS_EAGER_TRUNCATE_INDEX = 0x01;
// The upper bits are used to store length of extensions. DO NOT USE!
} // namespace RegionPersistFormat

using MaybeRegionPersistExtension = UInt32;
enum class RegionPersistExtension : MaybeRegionPersistExtension
{
    Reserved1 = 1,
    ReservedForTest = 2,
    // It should always be equal to the maximum supported type + 1
    MaxKnownFlag = 3,
};

constexpr MaybeRegionPersistExtension UNUSED_EXTENSION_NUMBER_FOR_TEST = UINT32_MAX / 2;
static_assert(!magic_enum::enum_contains<RegionPersistExtension>(UNUSED_EXTENSION_NUMBER_FOR_TEST));
static_assert(std::is_same_v<MaybeRegionPersistExtension, UInt32>);
static_assert(magic_enum::enum_underlying(RegionPersistExtension::MaxKnownFlag) <= UINT32_MAX / 2);

constexpr UInt32 Region::CURRENT_VERSION = static_cast<UInt64>(RegionPersistVersion::V2);

std::pair<MaybeRegionPersistExtension, UInt32> getPersistExtensionTypeAndLength(ReadBuffer & buf)
{
    auto ext_type = readBinary2<MaybeRegionPersistExtension>(buf);
    auto size = readBinary2<UInt32>(buf);
    // Note `ext_type` may not valid in RegionPersistExtension
    return std::make_pair(ext_type, size);
}

size_t writePersistExtension(
    UInt32 & cnt,
    WriteBuffer & wb,
    MaybeRegionPersistExtension ext_type,
    const char * data,
    UInt32 size)
{
    auto total_size = writeBinary2(ext_type, wb);
    total_size += writeBinary2(size, wb);
    wb.write(data, size);
    total_size += size;
    cnt++;
    return total_size;
}

std::tuple<size_t, UInt64> Region::serialize(WriteBuffer & buf) const
{
    auto binary_version = Region::CURRENT_VERSION;
    // Increase this when persist with a new extension type.
    UInt32 expected_extension_count = 0;
    using bundle_type = std::pair<int, int>;
    fiu_do_on(FailPoints::force_region_persist_version, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_persist_version); v)
        {
            std::tie(binary_version, expected_extension_count) = std::any_cast<bundle_type>(v.value());
            LOG_WARNING(
                Logger::get(),
                "Failpoint force_region_persist_version set region binary version, value={}, "
                "expected_extension_count={}",
                binary_version,
                expected_extension_count);
        }
    });
    size_t total_size = writeBinary2(binary_version, buf);
    UInt64 applied_index = -1;

    {
        std::shared_lock<std::shared_mutex> lock(mutex);

        // Serialize meta
        const auto [meta_size, index] = meta.serialize(buf);
        total_size += meta_size;
        applied_index = index;

        // Try serialize extra flags
        if (binary_version >= 2)
        {
            static_assert(sizeof(eager_truncated_index) == sizeof(UInt64));
            UInt32 flags = RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX;
            flags |= (expected_extension_count << 1);
            total_size += writeBinary2(flags, buf);
            total_size += writeBinary2(eager_truncated_index, buf);
        }

        UInt32 actual_extension_count = 0;
        fiu_do_on(FailPoints::force_region_persist_extension_field, {
            if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_persist_extension_field); v)
            {
                auto value = std::any_cast<int>(v.value());
                if (value & 1)
                {
                    std::string s = "abcd";
                    total_size += writePersistExtension(
                        actual_extension_count,
                        buf,
                        magic_enum::enum_underlying(RegionPersistExtension::ReservedForTest),
                        s.data(),
                        s.size());
                }
                if (value & 2)
                {
                    std::string s = "kkk";
                    total_size += writePersistExtension(
                        actual_extension_count,
                        buf,
                        UNUSED_EXTENSION_NUMBER_FOR_TEST,
                        s.data(),
                        s.size());
                }
                if (value & 4)
                {
                    std::string s = "zzz";
                    total_size += writePersistExtension(
                        actual_extension_count,
                        buf,
                        UNUSED_EXTENSION_NUMBER_FOR_TEST,
                        s.data(),
                        s.size());
                }
            }
        });
        RUNTIME_CHECK(
            expected_extension_count == actual_extension_count,
            expected_extension_count,
            actual_extension_count);

        // serialize data
        total_size += data.serialize(buf);
    }

    return {total_size, applied_index};
}

/// Currently supports:
/// 1. Vx -> Vy where x >= 2, y >= 3
/// 2. Vx -> V2 where x >= 2, in 7.5.0
/// 3. Vx -> V2 where x >= 2, in later 7.5
RegionPtr Region::deserialize(ReadBuffer & buf, const TiFlashRaftProxyHelper * proxy_helper)
{
    const auto binary_version = readBinary2<UInt32>(buf);
    auto current_version = Region::CURRENT_VERSION;
    fiu_do_on(FailPoints::force_region_read_version, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_read_version); v)
        {
            current_version = std::any_cast<UInt64>(v.value());
            LOG_WARNING(
                Logger::get(),
                "Failpoint force_region_read_version set region binary version, value={}",
                current_version);
        }
    });

    if (current_version <= 1 && binary_version > current_version)
    {
        // Conform to https://github.com/pingcap/tiflash/blob/43f809fffde22d0af4c519be4546a5bf4dde30a2/dbms/src/Storages/KVStore/Region.cpp#L197
        // When downgrade from x(where x > 1) -> 1, the old version will throw with "unexpected version".
        // So we will also throw here.
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Don't support downgrading from {} to {}",
            binary_version,
            current_version);
    }
    const auto binary_version_decoded = magic_enum::enum_cast<RegionPersistVersion>(binary_version);
    if (!binary_version_decoded.has_value())
    {
        LOG_DEBUG(DB::Logger::get(), "Maybe downgrade from {} to {}", binary_version, current_version);
    }

    // Deserialize meta
    RegionPtr region = std::make_shared<Region>(RegionMeta::deserialize(buf), proxy_helper);

    // Try deserialize flag
    if (binary_version >= 2)
    {
        auto flags = readBinary2<UInt32>(buf);
        if ((flags & RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX) != 0)
        {
            region->eager_truncated_index = readBinary2<UInt64>(buf);
        }
        UInt32 extension_cnt = flags >> 1;
        for (UInt32 i = 0; i < extension_cnt; i++)
        {
            auto [extension_type, length] = getPersistExtensionTypeAndLength(buf);
            bool debug_continue = false;
            using bundle_type = std::pair<int, std::shared_ptr<int>>;
            fiu_do_on(FailPoints::force_region_read_extension_field, {
                if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_read_extension_field); v)
                {
                    auto bundle = std::any_cast<bundle_type>(v.value());
                    if (bundle.first & 1)
                    {
                        if (extension_type == magic_enum::enum_underlying(RegionPersistExtension::ReservedForTest))
                        {
                            RUNTIME_CHECK(length == 4);
                            RUNTIME_CHECK(readStringWithLength(buf, 4) == "abcd");
                            debug_continue = true;
                            *(bundle.second) += 1;
                        }
                    }
                    if (bundle.first & 2)
                    {
                        RUNTIME_CHECK(length == 3);
                    }
                }
            });

            if (debug_continue)
            {
                continue;
            }

            //Throw away unknown extension data
            if (extension_type >= magic_enum::enum_underlying(RegionPersistExtension::MaxKnownFlag))
            {
                buf.ignore(length);
                continue;
            }

            RUNTIME_CHECK_MSG(false, "Unhandled extension, type={} length={}", extension_type, length);
        }
    }

    // deserialize data
    RegionData::deserialize(buf, region->data);
    region->data.reportAlloc(region->data.cf_data_size);

    // restore other var according to meta
    region->last_restart_log_applied = region->appliedIndex();
    region->setLastCompactLogApplied(region->appliedIndex());
    return region;
}

} // namespace DB
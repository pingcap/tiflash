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
    V3, // For flexible format
};

enum class RegionPersistFormat : UInt32
{
    EagerTruncate = 1, // Compat origin `HAS_EAGER_TRUNCATE_INDEX`
    Finished = 2, // Finished mark
    Unrecognizable = 3, // Can't recognize, maybe from recent versions
    TEST = 4,
};

constexpr UInt32 UNUSED_EXTENSION_NUMBER_FOR_TEST = 99999;
static_assert(!magic_enum::enum_contains<RegionPersistFormat>(UNUSED_EXTENSION_NUMBER_FOR_TEST));
static_assert(std::is_same_v<decltype(magic_enum::enum_underlying(RegionPersistFormat::EagerTruncate)), UInt32>);

const UInt32 Region::CURRENT_VERSION = static_cast<UInt64>(RegionPersistVersion::V3);

std::pair<RegionPersistFormat, UInt32> getPersistExtensionTypeAndLength(ReadBuffer & buf)
{
    auto flag_encoded = readBinary2<UInt32>(buf);
    auto maybe_flag = magic_enum::enum_cast<RegionPersistFormat>(flag_encoded);

    if (maybe_flag.has_value() && maybe_flag.value() == RegionPersistFormat::EagerTruncate)
    {
        // Compat
        return std::make_pair(RegionPersistFormat::EagerTruncate, sizeof(UInt64));
    }

    if (!maybe_flag.has_value())
    {
        auto size = readBinary2<UInt32>(buf);
        return std::make_pair(RegionPersistFormat::Unrecognizable, size);
    }
    auto flag = maybe_flag.value();
    UInt32 size = 0;
    if (flag == RegionPersistFormat::Finished)
    {
        // size = 0
    }
    else
    {
        size = readBinary2<UInt32>(buf);
    }
    return std::make_pair(flag, size);
}

size_t writePersistExtension(WriteBuffer & wb, UInt32 flag_encoded, const char * data, UInt32 size)
{
    auto total_size = writeBinary2(flag_encoded, wb);
    total_size += writeBinary2(size, wb);
    wb.write(data, size);
    total_size += size;
    return total_size;
}

size_t writePersistExtension(WriteBuffer & wb, RegionPersistFormat flag, const char * data, UInt32 size)
{
    RUNTIME_CHECK(flag != RegionPersistFormat::Finished);
    auto flag_encoded = magic_enum::enum_underlying(flag);
    if (flag == RegionPersistFormat::EagerTruncate)
    {
        // Compat
        auto total_size = writeBinary2(flag_encoded, wb);
        wb.write(data, size);
        total_size += size;
        return total_size;
    }
    else
    {
        return writePersistExtension(wb, flag_encoded, data, size);
    }
}

size_t writePersistExtensionSuffix(WriteBuffer & wb)
{
    return writeBinary2(magic_enum::enum_underlying(RegionPersistFormat::Finished), wb);
}

std::tuple<size_t, UInt64> Region::serialize(WriteBuffer & buf) const
{
    auto binary_version = Region::CURRENT_VERSION;
    fiu_do_on(FailPoints::force_region_persist_version, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_persist_version); v)
        {
            binary_version = std::any_cast<UInt64>(v.value());
            LOG_WARNING(
                Logger::get(),
                "Failpoint force_region_persist_version set region binary version, value={}",
                binary_version);
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
            WriteBufferFromOwnString sub_buf;
            auto sub_size = writeBinary2(eager_truncated_index, sub_buf);
            RUNTIME_CHECK(sub_size, sizeof(UInt64));
            auto s = sub_buf.releaseStr();
            total_size += writePersistExtension(buf, RegionPersistFormat::EagerTruncate, s.data(), s.size());
        }

        fiu_do_on(FailPoints::force_region_persist_extension_field, {
            if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_persist_extension_field); v)
            {
                auto value = std::any_cast<int>(v.value());
                if (value & 1)
                {
                    std::string s = "abcd";
                    total_size += writePersistExtension(buf, RegionPersistFormat::TEST, s.data(), s.size());
                }
                if (value & 2)
                {
                    std::string s = "kkk";
                    total_size += writePersistExtension(buf, UNUSED_EXTENSION_NUMBER_FOR_TEST, s.data(), s.size());
                }
                if (value & 4)
                {
                    std::string s = "zzz";
                    total_size += writePersistExtension(buf, UNUSED_EXTENSION_NUMBER_FOR_TEST, s.data(), s.size());
                }
            }
        });

        if (binary_version >= 3)
        {
            total_size += writePersistExtensionSuffix(buf);
        }
        // serialize data
        total_size += data.serialize(buf);
    }

    return {total_size, applied_index};
}

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

    if (current_version <= 1 && binary_version > current_version) {
        // Conform to https://github.com/pingcap/tiflash/blob/43f809fffde22d0af4c519be4546a5bf4dde30a2/dbms/src/Storages/KVStore/Region.cpp#L197
        // Includes only x(where x > 1) -> 1
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't support downgrading from {} to {}", binary_version, current_version);
    }
    const auto binary_version_decoded = magic_enum::enum_cast<RegionPersistVersion>(binary_version);
    // Before, it checks if the version is V1 or V2.
    if (!binary_version_decoded.has_value())
    {
        LOG_DEBUG(DB::Logger::get(), "Maybe downgrade from {} to {}", binary_version, current_version);
    }

    // Deserialize meta
    RegionPtr region = std::make_shared<Region>(RegionMeta::deserialize(buf), proxy_helper);

    // Try deserialize flag
    if (binary_version == 2)
    {
        // No Finished mark in version 2.
        auto flag = readBinary2<UInt32>(buf);
        if (magic_enum::enum_cast<RegionPersistFormat>(flag) == RegionPersistFormat::EagerTruncate)
        {
            region->eager_truncated_index = readBinary2<UInt64>(buf);
        }
    }
    else if (binary_version > 2)
    {
        while (true)
        {
            auto [flag, length] = getPersistExtensionTypeAndLength(buf);
            if (flag == RegionPersistFormat::Finished)
            {
                break;
            }
            if (flag == RegionPersistFormat::EagerTruncate)
            {
                region->eager_truncated_index = readBinary2<UInt64>(buf);
                continue;
            }

            // Hook in test before Unrecognizable
            bool debug_continue = false;
            using bundle_type = std::pair<int, std::shared_ptr<int>>;
            fiu_do_on(FailPoints::force_region_read_extension_field, {
                if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_region_read_extension_field); v)
                {
                    auto bundle = std::any_cast<bundle_type>(v.value());
                    if (bundle.first & 1)
                    {
                        if (flag == RegionPersistFormat::TEST)
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

            if (flag == RegionPersistFormat::Unrecognizable)
            {
                buf.ignore(length);
                continue;
            }

            RUNTIME_CHECK_MSG(false, "Unhandled extension {} length={}", magic_enum::enum_name(flag), length);
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
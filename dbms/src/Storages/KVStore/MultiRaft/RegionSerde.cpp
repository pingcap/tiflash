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
std::pair<MaybeRegionPersistExtension, UInt32> getPersistExtensionTypeAndLength(ReadBuffer & buf)
{
    auto ext_type = readBinary2<MaybeRegionPersistExtension>(buf);
    auto size = readBinary2<UInt32>(buf);
    // Note `ext_type` may not valid in RegionPersistExtension
    return std::make_pair(ext_type, size);
}

size_t Region::writePersistExtension(
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

std::tuple<size_t, UInt64> Region::serialize(WriteBuffer & buf, const RegionSerdeOpts & region_serde_opts) const
{
    auto expected_total = 0;
    std::optional<std::string> maybe_large_txn_seri = std::nullopt;
    if unlikely (region_serde_opts.large_txn_enabled)
    {
        maybe_large_txn_seri = data.serializeLargeTxnMeta();
        if unlikely (maybe_large_txn_seri.has_value())
        {
            expected_total += 1;
        }
    }
    return serializeImpl(
        RegionSerdeOpts::CURRENT_VERSION,
        expected_total,
        [&](UInt32 & actual_extension_count, WriteBuffer & buf) -> size_t {
            size_t total_size = 0;
            if unlikely (maybe_large_txn_seri.has_value())
            {
                total_size += Region::writePersistExtension(
                    actual_extension_count,
                    buf,
                    magic_enum::enum_underlying(RegionPersistExtension::LargeTxnDefaultCfMeta),
                    maybe_large_txn_seri.value().data(),
                    maybe_large_txn_seri.value().size());
            }
            return total_size;
        },
        buf,
        region_serde_opts);
}

std::tuple<size_t, UInt64> Region::serializeImpl(
    UInt32 binary_version,
    UInt32 expected_extension_count,
    std::function<size_t(UInt32 &, WriteBuffer &)> extra_handler,
    WriteBuffer & buf,
    const RegionSerdeOpts & region_serde_opts) const
{
    size_t total_size = writeBinary2(binary_version, buf);

    std::shared_lock<std::shared_mutex> lock(mutex);

    // Serialize meta
    const auto [meta_size, applied_index] = meta.serialize(buf);
    total_size += meta_size;

    // Try serialize extra flags
    if (binary_version >= 2)
    {
        static_assert(sizeof(eager_truncated_index) == sizeof(UInt64));
        // The upper 31 bits are used to store the length of extensions, and the lowest bit is flag of eager gc.
        UInt32 flags = (expected_extension_count << 1) | RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX;
        total_size += writeBinary2(flags, buf);
        total_size += writeBinary2(eager_truncated_index, buf);
    }

    UInt32 actual_extension_count = 0;
    total_size += extra_handler(actual_extension_count, buf);
    RUNTIME_CHECK(expected_extension_count == actual_extension_count, expected_extension_count, actual_extension_count);

    // serialize data
    total_size += data.serialize(buf, region_serde_opts);

    return {total_size, applied_index};
}

RegionPtr Region::deserialize(ReadBuffer & buf, const TiFlashRaftProxyHelper * proxy_helper)
{
    return Region::deserializeImpl(
        RegionSerdeOpts::CURRENT_VERSION,
        [](UInt32 type, ReadBuffer & buf, UInt32, RegionDeserResult & result) -> bool {
            if (type == magic_enum::enum_underlying(RegionPersistExtension::LargeTxnDefaultCfMeta))
            {
                result.large_txn_count = readBinary2<size_t>(buf);
                return true;
            }
            return false;
        },
        buf,
        proxy_helper);
}

/// Currently supports:
/// 1. Vx -> Vy where x >= 2, y >= 3
/// 2. Vx -> V2 where x >= 2, in later 7.5
/// 3. V2(7.5.x) -> V2(7.5.0), if no extensions. V2 may inherit some extensions from upper version, and failed to clean it before downgrade to 7.5.0.
RegionPtr Region::deserializeImpl(
    UInt32 current_version,
    std::function<bool(UInt32, ReadBuffer &, UInt32, RegionDeserResult &)> extra_handler,
    ReadBuffer & buf,
    const TiFlashRaftProxyHelper * proxy_helper)
{
    const auto binary_version = readBinary2<UInt32>(buf);
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

    RegionDeserResult deser_result;
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
            // Used in tests.
            if (extra_handler(extension_type, buf, length, deser_result))
                continue;
            // Throw away unknown extension data
            if (extension_type >= magic_enum::enum_underlying(RegionPersistExtension::MaxKnownFlag))
            {
                buf.ignore(length);
                continue;
            }

            RUNTIME_CHECK_MSG(false, "Unhandled extension, type={} length={}", extension_type, length);
        }
    }

    // deserialize data
    RegionData::deserialize(buf, region->data, deser_result);
    region->data.reportAlloc(region->data.cf_data_size);

    // restore other var according to meta
    region->last_restart_log_applied = region->appliedIndex();
    region->setLastCompactLogApplied(region->appliedIndex());
    return region;
}

} // namespace DB
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

namespace DB
{
using MaybeRegionPersistExtension = UInt32;
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

// The RegionPersistExtension has nothing to do with `version`.
// No matter upgrading or downgrading, we parse a `MaybeRegionPersistExtension` if we KNOW this field.
// We KNOW this field if it is LESS THAN `MaxKnownFlag`, so there should be NO hole before `MaxKnownFlag`.
// Once a extension is registered, what it's stand for shouldn't be changed. E.g. if Ext1 is assigned to 10, then in any older or newer version, we can't assign another Ext2 to 10.
enum class RegionPersistExtension : MaybeRegionPersistExtension
{
    ReservedForTest = 1,
    LargeTxnDefaultCfMeta = 2,
    // It should always be equal to the maximum supported type + 1
    MaxKnownFlag = 3,
};

/// The flexible pattern
/// The `payload 1` is of length defined by `length 1`
/// |--------- 32 bits ----------|
/// |- 31b exts -|- 1b eager gc -|
/// |--------- eager gc ---------|
/// |--------- eager gc ---------|
/// |-------- ext type 1 --------|
/// |--------- length 1 ---------|
/// |--------- payload 1 --------|
/// |--------- ......... --------|
/// |-------- ext type n --------|
/// |--------- length n ---------|
/// |--------- payload n --------|

constexpr MaybeRegionPersistExtension UNUSED_EXTENSION_NUMBER_FOR_TEST = UINT32_MAX / 2;
static_assert(!magic_enum::enum_contains<RegionPersistExtension>(UNUSED_EXTENSION_NUMBER_FOR_TEST));
static_assert(std::is_same_v<MaybeRegionPersistExtension, UInt32>);
static_assert(magic_enum::enum_underlying(RegionPersistExtension::MaxKnownFlag) <= UINT32_MAX / 2);
static_assert(
    magic_enum::enum_count<RegionPersistExtension>()
    == magic_enum::enum_underlying(RegionPersistExtension::MaxKnownFlag));
static_assert(RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX == 0x01);

struct RegionDeserResult
{
    size_t large_txn_count = 0;
};

struct RegionSerdeOpts
{
    constexpr static UInt32 CURRENT_VERSION = static_cast<UInt32>(RegionPersistVersion::V2);
    bool large_txn_enabled = false;
};

} // namespace DB
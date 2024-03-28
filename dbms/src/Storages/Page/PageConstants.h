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

#include <Core/Types.h>

namespace DB
{

static constexpr UInt64 MB = 1ULL * 1024 * 1024;
static constexpr UInt64 GB = MB * 1024;

enum class StorageType
{
    Unknown = 0,
    Log = 1,
    Data = 2,
    Meta = 3,
    KVStore = 4,
    RaftEngine = 5,
    KVEngine = 6,
    LocalKV = 7, // only stored on tiflash write node locally

    _MAX_STORAGE_TYPE_, // NOLINT(bugprone-reserved-identifier)
};

enum class PageStorageRunMode : UInt8
{
    ONLY_V2 = 1,
    ONLY_V3 = 2,
    MIX_MODE = 3,
    UNI_PS = 4,
};

// PageStorage V2 define
static constexpr UInt64 PAGE_SIZE_STEP = (1 << 10) * 16; // 16 KB
static constexpr UInt64 PAGE_FILE_MAX_SIZE = 1024 * 2 * MB;
static constexpr UInt64 PAGE_FILE_SMALL_SIZE = 2 * MB;
static constexpr UInt64 PAGE_FILE_ROLL_SIZE = 128 * MB;

static_assert(PAGE_SIZE_STEP >= ((1 << 10) * 16), "PAGE_SIZE_STEP should be at least 16 KB");
static_assert((PAGE_SIZE_STEP & (PAGE_SIZE_STEP - 1)) == 0, "PAGE_SIZE_STEP should be power of 2");

// PageStorage V3 define
static constexpr UInt64 BLOBFILE_LIMIT_SIZE = 256 * MB;
static constexpr UInt64 PAGE_META_ROLL_SIZE = 2 * MB;
static constexpr UInt64 MAX_PERSISTED_LOG_FILES = 4;

using NamespaceID = UInt64;
static constexpr NamespaceID MAX_NAMESPACE_ID = UINT64_MAX;
// KVStore stores it's data individually, so the actual `ns_id` value doesn't matter(just different from `MAX_NAMESPACE_ID` is enough)
static constexpr NamespaceID KVSTORE_NAMESPACE_ID = 1000000UL;
// just a random namespace id for test, the value doesn't matter
static constexpr NamespaceID TEST_NAMESPACE_ID = 1000;

} // namespace DB

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

#include <Core/Defines.h>
#include <IO/FileProvider/FileProvider_fwd.h>
#include <common/types.h>

namespace DB
{

struct SpillConfig
{
public:
    SpillConfig(
        const String & spill_dir_,
        const String & spill_id_,
        size_t max_cached_data_bytes_in_spiller_,
        size_t max_spilled_rows_per_file_,
        size_t max_spilled_bytes_per_file_,
        const FileProviderPtr & file_provider_,
        UInt64 for_all_constant_max_streams_ = 1,
        UInt64 for_all_constant_block_size_ = DEFAULT_BLOCK_SIZE);
    String spill_dir;
    String spill_id;
    String spill_id_as_file_name_prefix;
    /// soft limit of the max cached data bytes in spiller(used in CachedSpillHandler)
    size_t max_cached_data_bytes_in_spiller;
    /// soft limit of the max rows per spilled file
    UInt64 max_spilled_rows_per_file;
    /// soft limit of the max bytes per spilled file
    UInt64 max_spilled_bytes_per_file;
    FileProviderPtr file_provider;

    UInt64 for_all_constant_max_streams;
    UInt64 for_all_constant_block_size;
};
} // namespace DB

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

#include <Common/Exception.h>
#include <Interpreters/SettingsCommon.h>
#include <common/types.h>

namespace DB::PS::V3
{

enum class PageType : UInt8
{
    Normal = 0,
    // Raft logs are frequently added or deleted.
    RaftData = 1,
    // Local will not be upload to S3.
    Local = 2,

    // Support at most 10 types
    TypeCountLimit = 10,
};

struct PageTypeConfig
{
    SettingDouble heavy_gc_valid_rate = 0.5;
};

using PageTypes = std::vector<PageType>;
using PageTypeAndConfig = std::unordered_map<PageType, PageTypeConfig>;

struct PageTypeUtils
{
    static inline PageType getPageType(UInt64 file_id, bool treat_unknown_as_normal = true)
    {
        using T = std::underlying_type_t<PageType>;
        switch (file_id % static_cast<T>(PageType::TypeCountLimit))
        {
        case static_cast<T>(PageType::Normal):
            return PageType::Normal;
        case static_cast<T>(PageType::RaftData):
            return PageType::RaftData;
        case static_cast<T>(PageType::Local):
            return PageType::Local;
        default:
            return treat_unknown_as_normal ? PageType::Normal : PageType::TypeCountLimit;
        }
    }

    static inline UInt64 nextFileID(PageType page_type, UInt64 cur_max_id)
    {
        using T = std::underlying_type_t<PageType>;
        auto step = static_cast<T>(PageType::TypeCountLimit);
        auto id = cur_max_id + step;
        return id - (id % step) + static_cast<T>(page_type);
    }
};
} // namespace DB::PS::V3

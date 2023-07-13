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

#pragma once

#include <Common/Exception.h>
#include <Interpreters/SettingsCommon.h>
#include <common/types.h>

namespace DB::PS::V3
{

enum class PageType : UInt8
{
    Normal = 0,
    RaftData = 1,

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
        default:
            return treat_unknown_as_normal ? PageType::Normal : PageType::TypeCountLimit;
        }
    }

    static inline UInt64 firstFileID(PageType page_type)
    {
        using T = std::underlying_type_t<PageType>;
        RUNTIME_CHECK(page_type < PageType::TypeCountLimit);
        // 0 is invalid file id
        return (page_type == PageType::Normal) ? static_cast<T>(PageType::TypeCountLimit) : static_cast<T>(page_type);
    }

    static inline UInt64 nextFileID(PageType page_type, UInt64 cur_file_id)
    {
        using T = std::underlying_type_t<PageType>;
        RUNTIME_ASSERT(PageTypeUtils::getPageType(cur_file_id, false) == page_type);
        return cur_file_id + static_cast<T>(PageType::TypeCountLimit);
    }

    static inline PageTypes extractPageTypes(const PageTypeAndConfig & page_type_and_config)
    {
        PageTypes page_types;
        page_types.reserve(page_type_and_config.size());
        for (const auto & [page_type, _] : page_type_and_config)
        {
            page_types.emplace_back(page_type);
        }
        return page_types;
    }
};
} // namespace DB::PS::V3

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

#include <Storages/Page/PageDefines.h>

namespace DB::PS::V3
{
namespace u128
{
struct ExternalIdTrait
{
    using U64PageId = DB::PageId;
    using PageId = PageIdV3Internal;
    using Prefix = NamespaceId;

    static inline PageId getInvalidID()
    {
        return buildV3Id(0, DB::INVALID_PAGE_ID);
    }
    static inline U64PageId getU64ID(PageId page_id)
    {
        return page_id.low;
    }
    static inline Prefix getPrefix(const PageId & page_id)
    {
        return page_id.high;
    }
};
} // namespace u128
namespace universal
{
struct ExternalIdTrait
{
    using U64PageId = DB::PageId;
    using PageId = UniversalPageId;
    using Prefix = String;

    static inline PageId getInvalidID()
    {
        return "";
    }
    static inline U64PageId getU64ID(const PageId & /*page_id*/)
    {
        // FIXME: we need to ignore some page_id with prefix
        return 0;
    }
    static inline Prefix getPrefix(const PageId & /*page_id*/)
    {
        // FIXME
        return "";
    }
};
} // namespace universal
} // namespace DB::PS::V3

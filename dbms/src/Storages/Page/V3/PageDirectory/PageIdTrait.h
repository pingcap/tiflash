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

#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>

namespace DB::PS::V3
{
namespace u128
{
struct PageIdTrait
{
    using PageId = PageIdV3Internal;
    using Prefix = NamespaceID;

    static inline PageId getInvalidID() { return buildV3Id(0, DB::INVALID_PAGE_U64_ID); }
    static inline PageIdU64 getU64ID(const PageId & page_id) { return page_id.low; }
    static inline Prefix getPrefix(const PageId & page_id) { return page_id.high; }
    static inline PageIdU64 getPageMapKey(const PageId & page_id) { return page_id.low; }
    static inline size_t getPageIDSize(const PageId & page_id) { return sizeof(page_id); }
};
} // namespace u128
namespace universal
{
struct PageIdTrait
{
    using PageId = UniversalPageId;
    using Prefix = String;

    static inline PageId getInvalidID() { return UniversalPageId{}; }

    static PageIdU64 getU64ID(const PageId & page_id);

    static Prefix getPrefix(const PageId & page_id);

    static inline PageId getPageMapKey(const PageId & page_id) { return page_id; }

    static inline size_t getPageIDSize(const PageId & page_id) { return page_id.size(); }
};
} // namespace universal
} // namespace DB::PS::V3

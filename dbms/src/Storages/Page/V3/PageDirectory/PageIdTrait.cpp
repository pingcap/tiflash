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

#include <Storages/Page/V3/PageDirectory/PageIdTrait.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>

namespace DB::PS::V3::universal
{

PageIdU64 PageIdTrait::getU64ID(const PageIdTrait::PageId & page_id)
{
    return UniversalPageIdFormat::getU64ID(page_id);
}

PageIdTrait::Prefix PageIdTrait::getPrefix(const PageIdTrait::PageId & page_id)
{
    return UniversalPageIdFormat::getFullPrefix(page_id);
}

} // namespace DB::PS::V3::universal

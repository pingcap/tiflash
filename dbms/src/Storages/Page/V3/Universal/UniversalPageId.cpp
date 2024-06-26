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

#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>

namespace DB
{
UniversalPageId::~UniversalPageId()
{
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_sub(id.size());
}

UniversalPageId::UniversalPageId(UniversalPageId && other)
{
    // PS::PageStorageMemorySummary::uni_page_id_bytes has been set when `other` created
    id = std::move(other.id);
}
UniversalPageId::UniversalPageId(const UniversalPageId & other)
{
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_add(other.id.size());
    id = other.id;
}
UniversalPageId & UniversalPageId::operator=(UniversalPageId && other) noexcept
{
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_sub(id.size());
    id = std::move(other.id);
    return *this;
}
UniversalPageId & UniversalPageId::operator=(const UniversalPageId & other) noexcept
{
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_sub(id.size());
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_add(other.id.size());
    id = other.id;
    return *this;
}
UniversalPageId & UniversalPageId::operator=(String && id_) noexcept
{
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_sub(id.size());
    PS::PageStorageMemorySummary::uni_page_id_bytes.fetch_add(id_.size());
    id.swap(id_);
    return *this;
}
} // namespace DB

namespace DB::details
{

String UniversalPageIdFormatHelper::format(const DB::UniversalPageId & value)
{
    auto prefix = DB::UniversalPageIdFormat::getFullPrefix(value);
    if (value.hasPrefix({UniversalPageIdFormat::KV_PREFIX}) || value.hasPrefix({UniversalPageIdFormat::RAFT_PREFIX}))
    {
        return fmt::format("0x{}", Redact::keyToHexString(value.data(), value.size()));
    }
    return fmt::format(
        "0x{}.{}",
        Redact::keyToHexString(prefix.data(), prefix.size()),
        DB::UniversalPageIdFormat::getU64ID(value));
}

} // namespace DB::details

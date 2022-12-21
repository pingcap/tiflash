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

#include <IO/Endian.h>
#include <IO/WriteBuffer.h>
#include <Storages/Page/PageDefines.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
struct UniversalPageIdFormat
{
    static inline UInt64 encodeUInt64(const UInt64 x)
    {
        return toBigEndian(x);
    }

    static inline void encodeUInt64(const UInt64 x, WriteBuffer & ss)
    {
        auto u = UniversalPageIdFormat::encodeUInt64(x);
        ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
    }

    template <typename T>
    static inline T read(const char * s)
    {
        return *(reinterpret_cast<const T *>(s));
    }

    static inline UInt64 decodeUInt64(const char * s)
    {
        auto v = read<UInt64>(s);
        return toBigEndian(v);
    }
};
}

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
    static inline U64PageId getPageMapKey(const PageId & page_id)
    {
        return page_id.low;
    }
    // Used for indicate infinite end
    static inline bool isInvalidPageId(const PageId & page_id)
    {
        return page_id == 0;
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
    static inline U64PageId getU64ID(const PageId & page_id)
    {
        // Only count id with known prefix
        // TODO: remove hardcode
        if (startsWith(page_id, "t_l_") || startsWith(page_id, "t_d_") || startsWith(page_id, "t_m_"))
        {
            return DB::UniversalPageIdFormat::decodeUInt64(page_id.data() + page_id.size() - sizeof(UInt64));
        }
        else
        {
            return 0;
        }
    }
    static inline Prefix getPrefix(const PageId & page_id)
    {
        return page_id.substr(0, page_id.size() - sizeof(UInt64));
    }
    static inline PageId getPageMapKey(const PageId & page_id)
    {
        return page_id;
    }
    static inline bool isInvalidPageId(const PageId & page_id)
    {
        return page_id.empty();
    }
};
} // namespace universal
} // namespace DB::PS::V3

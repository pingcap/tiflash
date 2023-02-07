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

#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormat.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatch.h>
#include <Storages/Page/WriteBatch.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

class UniversalWriteBatchAdaptor : private boost::noncopyable
{
public:
    explicit UniversalWriteBatchAdaptor(String && prefix_)
        : prefix(std::move(prefix_))
    {}

    UniversalWriteBatchAdaptor(UniversalWriteBatchAdaptor && rhs)
        : uwb(std::move(rhs.uwb))
        , prefix(std::move(rhs.prefix))
    {}

    void putPage(PageIdU64 page_id, UInt64 tag, const ReadBufferPtr & read_buffer, PageSize size, const PageFieldSizes & data_sizes = {})
    {
        uwb.putPage(UniversalPageId::toFullPageId(prefix, page_id), tag, read_buffer, size, data_sizes);
    }

    void putExternal(PageIdU64 page_id, UInt64 tag)
    {
        uwb.putExternal(UniversalPageId::toFullPageId(prefix, page_id), tag);
    }

    // Add RefPage{ref_id} -> Page{page_id}
    void putRefPage(PageIdU64 ref_id, PageIdU64 page_id)
    {
        uwb.putRefPage(UniversalPageId::toFullPageId(prefix, ref_id), UniversalPageId::toFullPageId(prefix, page_id));
    }

    void delPage(PageIdU64 page_id)
    {
        uwb.delPage(UniversalPageId::toFullPageId(prefix, page_id));
    }

    bool empty() const
    {
        return uwb.empty();
    }

    void clear()
    {
        uwb.clear();
    }

    const UniversalWriteBatch & getUniversalWriteBatch()
    {
        return uwb;
    }

    UniversalWriteBatch && releaseUniversalWriteBatch()
    {
        return std::move(uwb);
    }

private:
    UniversalWriteBatch uwb;
    String prefix;
};
} // namespace DB

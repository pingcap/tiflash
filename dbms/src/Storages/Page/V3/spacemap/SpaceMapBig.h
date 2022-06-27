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
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <fmt/format.h>

#include <ext/shared_ptr_helper.h>
#include <map>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace PS::V3
{
// A space map that is designed for holding only one large page data (size > blobstore.config.file_limit_size)
class BigSpaceMap
    : public SpaceMap
    , public ext::SharedPtrHelper<BigSpaceMap>
{
public:
    ~BigSpaceMap() override = default;

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> /*checker*/, size_t /*size*/) override
    {
        // Can't do check
        return true;
    }

protected:
    BigSpaceMap(UInt64 start, UInt64 end)
        : SpaceMap(start, end, SMAP64_BIG)
    {
        if (start != 0)
        {
            throw Exception(fmt::format("[start={}] is not zero. We should not use [type=SMAP64_BIG] to do that.", start), //
                            ErrorCodes::LOGICAL_ERROR);
        }
    }

    String toDebugString() override
    {
        FmtBuffer fmt_buffer;
        fmt_buffer.append("    BIG-Map entries status: \n");
        fmt_buffer.fmtAppend("      Single Space start: 0 size : {}\n", size_in_used);

        return fmt_buffer.toString();
    }

    std::pair<UInt64, UInt64> getSizes() const override
    {
        if (size_in_used == 0)
        {
            throw Exception("size_in_used is zero. it should not happend", //
                            ErrorCodes::LOGICAL_ERROR);
        }
        return std::make_pair(size_in_used, size_in_used);
    }

    UInt64 getUsedBoundary() override
    {
        return end;
    }

    bool isMarkUnused(UInt64 /*offset*/, size_t /*length*/) override
    {
        return true;
    }

    bool markUsedImpl(UInt64 offset, size_t length) override
    {
        if (offset != 0)
        {
            throw Exception(fmt::format("[offset={}] is not zero. We should not use [type=SMAP64_BIG] to do that.", offset), //
                            ErrorCodes::LOGICAL_ERROR);
        }

        if (length < end)
        {
            throw Exception(fmt::format("[length={}] less than [end={}]. We should not use [type=SMAP64_BIG] to do that.", length, end), //
                            ErrorCodes::LOGICAL_ERROR);
        }

        size_in_used = length;

        return true;
    }

    std::tuple<UInt64, UInt64, bool> searchInsertOffset(size_t size) override
    {
        if (size < end)
        {
            throw Exception(fmt::format("[size={}] less than [end={}]. We should not use [type=SMAP64_BIG] to do that.", size, end), //
                            ErrorCodes::LOGICAL_ERROR);
        }
        size_in_used = size;

        // offset : from start 0
        // max_cap : will be 0
        // is expansion : true(or false, both ok)
        return std::make_tuple(0, 0, true);
    }

    UInt64 updateAccurateMaxCapacity() override
    {
        // Won't update max_cap.
        return 0;
    }

    bool markFreeImpl(UInt64 offset, size_t length) override
    {
        if (length != size_in_used)
        {
            throw Exception(fmt::format("[length={}] less than [end={}]. We should not use [type=SMAP64_BIG] to do that.", length, end), //
                            ErrorCodes::LOGICAL_ERROR);
        }

        if (offset != 0)
        {
            throw Exception(fmt::format("[offset={}] is not zero. We should not use [type=SMAP64_BIG] to do that.", offset), //
                            ErrorCodes::LOGICAL_ERROR);
        }

        return true;
    }

private:
    UInt64 size_in_used = 0;
};

using BigSpaceMapPtr = std::shared_ptr<BigSpaceMap>;

}; // namespace PS::V3
}; // namespace DB
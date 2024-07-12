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

#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/Region.h>

namespace DB
{
Region::CommittedScanner::CommittedScanner(const RegionPtr & region_, bool use_lock, bool need_value)
    : region(region_)
    , need_val(need_value)
{
    if (use_lock)
        lock = std::shared_lock<std::shared_mutex>(region_->mutex);

    const auto & data = region->data.writeCF().getData();

    write_map_size = data.size();
    write_map_it = data.begin();
    write_map_it_end = data.end();

    hard_error = true;
    if (region->getClusterRaftstoreVer() == DB::RaftstoreVer::V2)
    {
        hard_error = false;
    }
}

bool Region::CommittedScanner::hasNext()
{
    if (peeked)
    {
        return true;
    }
    else
    {
        if (write_map_size == 0)
            return false;
        while (write_map_it != write_map_it_end)
        {
            if (tryNext())
            {
                return true;
            }
        }
        return false;
    }
}

bool Region::CommittedScanner::tryNext()
{
    RUNTIME_CHECK_MSG(!peeked.has_value(), "Can't call CommittedScanner::tryNext() twice");
    assert(write_map_it != write_map_it_end);
    auto ans = region->readDataByWriteIt(write_map_it++, need_val, hard_error);
    if (ans)
    {
        peeked = std::move(ans.value());
        return true;
    }
    else
    {
        // Returning nullopt only means we encoutered a orphan write key.
        return false;
    }
}

RegionDataReadInfo Region::CommittedScanner::next()
{
    RUNTIME_CHECK_MSG(peeked.has_value(), "Call CommittedScanner::next() with empty prefetched value");
    auto res = std::move(peeked.value());
    peeked.reset();
    return res;
}
} // namespace DB

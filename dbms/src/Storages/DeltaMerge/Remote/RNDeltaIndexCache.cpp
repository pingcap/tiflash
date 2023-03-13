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

#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache.h>

namespace DB::DM::Remote
{

DeltaIndexPtr RNDeltaIndexCache::getDeltaIndex(const RNDeltaIndexCache::CacheKey & key)
{
    auto [index_ptr, _] = cache.getOrSet(key, [] {
        auto index = std::make_shared<DeltaIndex>();
        return index;
    });
    return index_ptr;
}

} // namespace DB::DM::Remote

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

#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/Remote/LocalPageCache.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class Manager;
using ManagerPtr = std::shared_ptr<Manager>;

class Manager : private boost::noncopyable
{
public:
    explicit Manager(const Context & global_context, String nfs_directory);

    LocalPageCachePtr getPageCache() const
    {
        return page_cache;
    }

    IDataStorePtr getDataStore() const
    {
        return data_store;
    }

private:
    LocalPageCachePtr page_cache;
    IDataStorePtr data_store;
};

} // namespace DB::DM::Remote

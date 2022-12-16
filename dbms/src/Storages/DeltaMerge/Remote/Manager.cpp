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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreNFS.h>
#include <Storages/DeltaMerge/Remote/Manager.h>

namespace DB::DM::Remote
{

Manager::Manager(const Context & global_context, String nfs_directory)
    : page_cache(std::make_shared<LocalPageCache>(global_context)) // TODO: Write Node does not need this. We need to refine the interface..
    , data_store(std::make_shared<DataStoreNFS>(
          DataStoreNFS::Config{
              .base_directory = nfs_directory,
          },
          global_context.getFileProvider()))
{}

} // namespace DB::DM::Remote

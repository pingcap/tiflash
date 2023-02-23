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

#include <Common/Allocator.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/Remote/RemoteDataInfo.h>
#include <Storages/Page/V3/PageEntry.h>


namespace DB
{
class RemotePageReader : private Allocator<false>
{
public:
    explicit RemotePageReader(const String & remote_directory_)
        :remote_directory(remote_directory_)
    {}

    Page read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry);

    Page read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry, std::vector<size_t> fields);

private:
    String remote_directory;
};

using RemotePageReaderPtr = std::shared_ptr<RemotePageReader>;
}

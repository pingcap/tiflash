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

#include <IO/BufferBase.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>

#include <map>
#include <set>
#include <unordered_map>


namespace DB
{

class UniversalPage : public OwningPageData
{
public:
    explicit UniversalPage()
        : UniversalPage("")
    {}

    explicit UniversalPage(const UniversalPageId & page_id_)
        : page_id(page_id_)
    {
    }

    UniversalPageId page_id;

    inline bool isValid() const { return !page_id.empty(); }
};

using UniversalPages = std::vector<UniversalPageId>;
using UniversalPageMap = std::map<UniversalPageId, UniversalPage>;

} // namespace DB

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

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Page/PageDefines.h>
#include <common/strong_typedef.h>
#include <fmt/core.h>

namespace DB::DM
{

struct ExternalDTFileInfo
{
    /**
     * The allocated PageId of the file.
     */
    PageId id;

    /**
     * The handle range of contained data.
     */
    RowKeyRange range;

    ExternalDTFileInfo(PageId id_, RowKeyRange range_)
        : id(id_)
        , range(range_)
    {}

    std::string toString() const
    {
        return fmt::format(
            "ExternalDTFile {{ id = {}, range = {} }}",
            id,
            range.toDebugString());
    }
};

STRONG_TYPEDEF(std::vector<ExternalDTFileInfo>, SortedExternalDTFileInfos)

} // namespace DB::DM

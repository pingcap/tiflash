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

#pragma once

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Page/PageDefinesBase.h>
#include <fmt/core.h>

namespace DB::DM
{

struct ExternalDTFileInfo
{
    /**
     * The allocated PageId of the file.
     */
    PageIdU64 id;

    /**
     * The handle range of contained data.
     */
    RowKeyRange range;

    std::string toString() const { return fmt::format("<file=dmf_{} range={}>", id, range.toDebugString()); }
};

} // namespace DB::DM

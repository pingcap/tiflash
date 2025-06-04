// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/KVStore/Region_fwd.h>

namespace DB
{

struct CheckpointIngestInfo;
using CheckpointIngestInfoPtr = std::shared_ptr<CheckpointIngestInfo>;

// A wrap of RegionPtr, with snapshot files directory waiting to be ingested
struct RegionPtrWithSnapshotFiles
{
    using Base = RegionPtr;

    /// can accept const ref of RegionPtr without cache
    RegionPtrWithSnapshotFiles( // NOLINT(google-explicit-constructor)
        const Base & base_,
        std::vector<DM::ExternalDTFileInfo> && external_files_ = {});

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; } // NOLINT(google-explicit-constructor)

    const Base & base;
    const std::vector<DM::ExternalDTFileInfo> external_files;
};

// A wrap of RegionPtr, with checkpoint info to be ingested
struct RegionPtrWithCheckpointInfo
{
    using Base = RegionPtr;

    RegionPtrWithCheckpointInfo(const Base & base_, CheckpointIngestInfoPtr checkpoint_info_);

    /// to be compatible with usage as RegionPtr.
    Base::element_type * operator->() const { return base.operator->(); }
    const Base::element_type & operator*() const { return base.operator*(); }

    /// make it could be cast into RegionPtr implicitly.
    operator const Base &() const { return base; } // NOLINT(google-explicit-constructor)

    const Base & base;
    CheckpointIngestInfoPtr checkpoint_info;
};

} // namespace DB

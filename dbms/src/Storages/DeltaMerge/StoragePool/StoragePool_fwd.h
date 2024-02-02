// Copyright 2024 PingCAP, Inc.
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

#include <memory>

namespace DB::DM
{
class GlobalPageIdAllocator;
using GlobalPageIdAllocatorPtr = std::shared_ptr<GlobalPageIdAllocator>;

class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;

class GlobalStoragePool;
using GlobalStoragePoolPtr = std::shared_ptr<GlobalStoragePool>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

static constexpr std::chrono::seconds DELTA_MERGE_GC_PERIOD(60);

} // namespace DB::DM

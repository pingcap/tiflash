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

#include <memory>

namespace TiDB
{
struct ColumnInfo;
}

namespace DB::DM
{
<<<<<<< HEAD:dbms/src/Storages/DeltaMerge/StoragePool_fwd.h

class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;

class GlobalStoragePool;
using GlobalStoragePoolPtr = std::shared_ptr<GlobalStoragePool>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

=======
struct ColumnDefine;
using ColumnDefines = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;
using ColumnDefineMap = std::unordered_map<DB::ColumnID, ColumnDefine>;

using ColumnInfos = std::vector<TiDB::ColumnInfo>;
>>>>>>> e6fc04addf (Storages: Fix obtaining incorrect column information when there are virtual columns in the query (#9189)):dbms/src/Storages/DeltaMerge/ColumnDefine_fwd.h
} // namespace DB::DM

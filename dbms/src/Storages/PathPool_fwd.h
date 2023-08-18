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

#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Core/Types.h>
#include <Storages/Page/PageDefinesBase.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

/// A class to manage global paths.
class PathPool;
/// A class to manage paths for the specified storage.
class StoragePathPool;

/// ===== Delegators to StoragePathPool ===== ///
/// Delegators to StoragePathPool. Use for managing the path of DTFiles.
class StableDiskDelegator;
/// Delegators to StoragePathPool. Use by PageStorage for managing the path of PageFiles.
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;

} // namespace DB

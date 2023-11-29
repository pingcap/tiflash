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
#include <common/types.h>

#include <memory>

namespace DB
{
class ParsedCheckpointDataHolder;
using ParsedCheckpointDataHolderPtr = std::shared_ptr<ParsedCheckpointDataHolder>;
class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

struct CheckpointInfo
{
    UInt64 remote_store_id = 0;
    UInt64 region_id = 0;
    ParsedCheckpointDataHolderPtr checkpoint_data_holder; // a wrapper to protect the path of `temp_ps` to be deleted
    UniversalPageStoragePtr temp_ps;
};
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;
} // namespace DB
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

#include <Core/Types.h>

#include <optional>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop

namespace TiDB
{

struct TiFlashReplicaInfo
{
    UInt64 count = 0;

    /// Fields below are useless for tiflash now.
    // Strings location_labels
    // bool available
    // std::vector<Int64> available_partition_ids;

    Poco::JSON::Object::Ptr getJSONObject() const;
    void deserialize(Poco::JSON::Object::Ptr & json);
};
} // namespace TiDB
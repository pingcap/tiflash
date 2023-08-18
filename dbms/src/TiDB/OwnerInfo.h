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

namespace DB
{

enum class OwnerType
{
    // This node is owner
    IsOwner,
    // This node is not owner
    NotOwner,

    // Errors
    // There is no owner at the moment
    NoLeader,
    // Grpc error happens
    GrpcError,
};

struct OwnerInfo
{
    // This node is owner or not
    OwnerType status = OwnerType::NoLeader;
    // The id of owner. Usually it is the service url.
    // When status is `IsOwner`/`NotOwner`, return the owner_id.
    // When status is `GrpcError`, reuse this field to return the error message.
    String owner_id;

    const String & errMsg() const
    {
        assert(status == OwnerType::GrpcError);
        return owner_id;
    }
};

} // namespace DB

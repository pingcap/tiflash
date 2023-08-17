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

#include <Common/Exception.h>
#include <TiDB/OwnerManager.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{

/// Mock owner manager for testing
class MockOwnerManager : public OwnerManager
{
public:
    explicit MockOwnerManager(std::string_view id_)
        : id(id_)
        , owner(OwnerType::NoLeader)
    {}

    void campaignOwner() override
    {
        // always win
        owner = OwnerType::IsOwner;
        actual_owner_id = id;
    }

    // Quick check whether this node is owner or not
    bool isOwner() override { return owner == OwnerType::IsOwner; }

    OwnerInfo getOwnerID() override
    {
        return OwnerInfo{
            .status = owner,
            .owner_id = actual_owner_id,
        };
    }

    bool resignOwner() override
    {
        if (owner != OwnerType::IsOwner)
        {
            return false;
        }

        // no owner now
        owner = OwnerType::NoLeader;
        actual_owner_id.clear();
        return true;
    }

    void cancel() override
    {
        // other node win
        owner = OwnerType::NotOwner;
    }

    void setBeOwnerHook(std::function<void()> &&) override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, ""); }


    // testing method
    void setWinOwnerID(std::optional<std::string_view> owner_id)
    {
        if (!owner_id)
        {
            owner = OwnerType::NoLeader;
        }
        else if (id == owner_id.value())
        {
            // this node win
            owner = OwnerType::IsOwner;
            actual_owner_id = owner_id.value();
        }
        else
        {
            RUNTIME_CHECK(!owner_id->empty());
            actual_owner_id = owner_id.value();
            owner = OwnerType::NotOwner;
        }
    }

private:
    String id;
    OwnerType owner;

    String actual_owner_id;
};

} // namespace DB

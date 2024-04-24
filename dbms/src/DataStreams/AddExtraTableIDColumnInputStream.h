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

#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/KVStore/Types.h>

namespace DB
{

/**
  * Adds an extra TableID column to the block.
  */
class AddExtraTableIDColumnInputStream : public IBlockInputStream
{
    static constexpr auto NAME = "AddExtraTableIDColumn";

public:
    AddExtraTableIDColumnInputStream(BlockInputStreamPtr input, int extra_table_id_index, TableID physical_table_id);

    String getName() const override { return NAME; }

    Block getHeader() const override { return action.getHeader(); }

protected:
    Block read() override;

private:
    const TableID physical_table_id;
    AddExtraTableIDColumnTransformAction action;
};

} // namespace DB

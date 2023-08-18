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

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Storages/IStorage.h>


namespace DB
{


/** Writes data to the specified table and to all dependent materialized views.
  */
class PushingToViewsBlockOutputStream : public IBlockOutputStream
{
public:
    PushingToViewsBlockOutputStream(
        const String & database,
        const String & table,
        const StoragePtr & storage,
        const Context & context_,
        const ASTPtr & query_ptr_,
        bool no_destination = false);

    Block getHeader() const override { return storage->getSampleBlock(); }
    void write(const Block & block) override;

    void flush() override
    {
        if (output)
            output->flush();
    }

    void writePrefix() override
    {
        if (output)
            output->writePrefix();
    }

    void writeSuffix() override
    {
        if (output)
            output->writeSuffix();
    }

private:
    StoragePtr storage;
    BlockOutputStreamPtr output;

    const Context & context;
    ASTPtr query_ptr;
};


} // namespace DB

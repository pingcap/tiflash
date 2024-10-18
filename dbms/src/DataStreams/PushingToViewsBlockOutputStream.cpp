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

#include <Common/Exception.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/IStorage.h>


namespace DB
{
PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(
    const String & database,
    const String & table,
    const StoragePtr & storage,
    const Context & context_,
    const ASTPtr & query_ptr_,
    bool no_destination)
    : context(context_)
    , query_ptr(query_ptr_)
{
    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    addTableLock(storage->lockForShare(context.getCurrentQueryId()));

    if (!table.empty())
    {
        Dependencies dependencies = context.getDependencies(database, table);

        RUNTIME_CHECK_MSG(dependencies.empty(), "Do not support ClickHouse's materialized view");
    }

    /* Do not push to destination table if the flag is set */
    if (!no_destination)
    {
        output = storage->write(query_ptr, context.getSettingsRef());
    }
}


void PushingToViewsBlockOutputStream::write(const Block & block)
{
    if (output)
        output->write(block);
}

} // namespace DB

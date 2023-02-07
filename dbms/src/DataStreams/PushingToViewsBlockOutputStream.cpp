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

#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>


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

        /// We need special context for materialized views insertions
        if (!dependencies.empty())
        {
            views_context = std::make_unique<Context>(context);
        }

        for (const auto & database_table : dependencies)
        {
            auto dependent_table = context.getTable(database_table.first, database_table.second);
            const auto & materialized_view = dynamic_cast<const StorageMaterializedView &>(*dependent_table);

            auto query = materialized_view.getInnerQuery();
            BlockOutputStreamPtr out = std::make_shared<PushingToViewsBlockOutputStream>(
                database_table.first,
                database_table.second,
                dependent_table,
                *views_context,
                ASTPtr());
            views.emplace_back(ViewInfo{std::move(query), database_table.first, database_table.second, std::move(out)});
        }
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

    /// Insert data into materialized views only after successful insert into main table
    for (auto & view : views)
    {
        try
        {
            BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
            InterpreterSelectQuery select(view.query, *views_context, {}, QueryProcessingStage::Complete, 0, from);
            BlockInputStreamPtr in = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
            /// Squashing is needed here because the materialized view query can generate a lot of blocks
            /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
            /// and two-level aggregation is triggered).
            in = std::make_shared<SquashingBlockInputStream>(
                in,
                context.getSettingsRef().min_insert_block_size_rows,
                context.getSettingsRef().min_insert_block_size_bytes,
                /*req_id=*/"");

            in->readPrefix();

            while (Block result_block = in->read())
                view.out->write(result_block);

            in->readSuffix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while pushing to view " + view.database + "." + view.table);
            throw;
        }
    }
}

} // namespace DB

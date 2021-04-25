#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace ClusterProxy
{

SelectStreamFactory::SelectStreamFactory(
    const Block & header,
    QueryProcessingStage::Enum processed_stage_,
    QualifiedTableName main_table_,
    const Tables & external_tables_)
    : header(header),
    processed_stage{processed_stage_},
    main_table(std::move(main_table_)),
    external_tables{external_tables_}
{
}

namespace
{

BlockInputStreamPtr createLocalStream(const ASTPtr & query_ast, const Context & context, QueryProcessingStage::Enum processed_stage)
{
    InterpreterSelectQuery interpreter{query_ast, context, {}, processed_stage};
    BlockInputStreamPtr stream = interpreter.execute().in;

    /** Materialization is needed, since from remote servers the constants come materialized.
      * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
      * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
      */
    return std::make_shared<MaterializingBlockInputStream>(stream);
}

}

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const String & query, const ASTPtr & query_ast,
    const Context & context, const ThrottlerPtr & throttler,
    BlockInputStreams & res)
{
    auto emplace_local_stream = [&]()
    {
        res.emplace_back(createLocalStream(query_ast, context, processed_stage));
    };

    auto emplace_remote_stream = [&]()
    {
        auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, header, context, nullptr, throttler, external_tables, processed_stage);
        stream->setPoolMode(PoolMode::GET_MANY);
        stream->setMainTable(main_table);
        res.emplace_back(std::move(stream));
    };

    if (shard_info.isLocal())
    {
        StoragePtr main_table_storage = context.tryGetTable(main_table.database, main_table.table);
        if (!main_table_storage) /// Table is absent on a local server.
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
            if (shard_info.pool)
            {
                LOG_WARNING(
                        &Logger::get("ClusterProxy::SelectStreamFactory"),
                        "There is no table " << main_table.database << "." << main_table.table
                        << " on local replica of shard " << shard_info.shard_num << ", will try remote replicas.");

                emplace_remote_stream();
                return;
            }
            else
            {
                /// Let it fail the usual way.
                emplace_local_stream();
                return;
            }
        }

        /// Table is not replicated, use local server.
        emplace_local_stream();
        return;
    }
    else
        emplace_remote_stream();
}

}
}
